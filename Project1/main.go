package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	//Shortest job first scheduling
	SJFSchedule(os.Stdout, "Shortest-job-first", processes)

	//Shortest job priority sscheduing
	SJFPrioritySchedule(os.Stdout, "Priority", processes)

	// Round robin Scheduling
	RRSchedule(os.Stdout, "Round-robin", processes, 10)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     = make([]int64, len(processes))
		remainingTime   = make([]int64, len(processes))
		schedule        = make([][]string, 0)
		gantt           = make([]TimeSlice, 0)
	)

	// Remaining time set to burst duration
	for i, p := range processes {
		remainingTime[i] = p.BurstDuration
	}

	for serviceTime < lastArrivalTime(processes) || len(schedule) < len(processes) {
		var (
			selected  = -1
			Shortest  = math.MaxInt64
			completed = 0
		)

		//Selecting the process with the shortest burst
		for i := range processes {
			if processes[i].ArrivalTime <= serviceTime && remainingTime[i] > 0 && processes[i].Priority < int64(Shortest) {
				selected = i
				Shortest = int(processes[i].Priority)
			}
		}

		if selected >= 0 {
			if waitingTime[selected] == 0 {
				waitingTime[selected] = serviceTime - processes[selected].ArrivalTime
			}

			if !containsPID(schedule, processes[selected].ProcessID) {
				schedule = append(schedule, []string{
					fmt.Sprint(processes[selected].ProcessID),
					fmt.Sprint(processes[selected].Priority),
					fmt.Sprint(processes[selected].BurstDuration),
					fmt.Sprint(processes[selected].ArrivalTime),
					fmt.Sprint(waitingTime[selected]),
					fmt.Sprint(totalTurnaround + float64(serviceTime-processes[selected].ArrivalTime)),
					fmt.Sprint(totalTurnaround + float64(serviceTime-processes[selected].ArrivalTime+processes[selected].BurstDuration)),
				})
			}

			if remainingTime[selected] > 1 {
				remainingTime[selected]--
			} else {
				remainingTime[selected] = 0
				completed = 1
				totalTurnaround += float64(serviceTime - processes[selected].ArrivalTime + 1)
				lastCompletion = float64(serviceTime + 1)
			}

			gantt = append(gantt, TimeSlice{
				PID:   processes[selected].ProcessID,
				Start: serviceTime,
				Stop:  serviceTime + 1,
			})
		} else {
			serviceTime++
		}

		for i := range processes {
			if processes[i].ArrivalTime == serviceTime && remainingTime[i] > 0 && !containsPID(schedule, processes[i].ProcessID) {
				waitingTime[i] = 0
			}
		}

		if completed == 1 {
			selected = -1
			Shortest = math.MaxInt64

			for i := range processes {
				if processes[i].ArrivalTime <= serviceTime && remainingTime[i] > 0 && processes[i].Priority < int64(Shortest) {
					selected = i
					Shortest = int(processes[i].Priority)
				}
			}
		}
	}

	count := float64(len(processes))
	averageWait := totalTurnaround / count
	averageThroughput := count / lastCompletion
	averageTurnaround := totalTurnaround / count

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, averageWait, averageTurnaround, averageThroughput)
}

// Shortest job first priority scheduler
func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	// Sorting the process by the shortes job first
	sort.Slice(processes, func(i, j int) bool {
		return processes[i].BurstDuration < processes[j].BurstDuration
	})

	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}

		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}

		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// func RRSchedule(w io.Writer, title string, processes []Process) { }

func RRSchedule(w io.Writer, title string, processes []Process, timeQuantum int64) {

	var (
		serviceTime     int64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     = make([]int64, len(processes))
		remainingTime   = make([]int64, len(processes))
		schedule        = make([][]string, 0)
		gantt           = make([]TimeSlice, 0)
	)

	// Setting the remaining time to burst duration of every process
	for i, p := range processes {
		remainingTime[i] = p.BurstDuration
	}

	// Round robin process execution below
	for serviceTime < lastArrivalTime(processes) || len(schedule) < len(processes) {
		completed := false

		// Processing all that arrived before the current service time
		for i := range processes {
			if processes[i].ArrivalTime <= serviceTime && remainingTime[i] > 0 {
				//Begin a new process
				if waitingTime[i] == 0 {
					waitingTime[i] = serviceTime - processes[i].ArrivalTime
				}

				// Add the processes to the schedule
				if !containsPID(schedule, processes[i].ProcessID) {
					schedule = append(schedule, []string{
						fmt.Sprint(processes[i].ProcessID),
						fmt.Sprint(processes[i].Priority),
						fmt.Sprint(processes[i].BurstDuration),
						fmt.Sprint(processes[i].ArrivalTime),
						fmt.Sprint(totalTurnaround),
						fmt.Sprint(totalTurnaround + float64(processes[i].ArrivalTime)),
					})
				}

				//Here we check the given processes for
				if remainingTime[i] > timeQuantum {
					serviceTime += timeQuantum
					remainingTime[i] -= timeQuantum
				} else {
					serviceTime += remainingTime[i]
					totalTurnaround += float64(serviceTime - processes[i].ArrivalTime)
					remainingTime[i] = 0
					completed = true
				}

				//Adding to our gantt chart
				gantt = append(gantt, TimeSlice{
					PID:   processes[i].ProcessID,
					Start: serviceTime - timeQuantum,
					Stop:  serviceTime,
				})
			}
		}

		// Moving to next process if none were completed
		if !completed {
			serviceTime++
		}

		for i := range processes {
			if processes[i].ArrivalTime == serviceTime && remainingTime[i] > 0 && !containsPID(schedule, processes[i].ProcessID) {
				waitingTime[i] = 0
			}
		}
	}

	count := float64(len(processes))
	averageWait := totalTurnaround / count
	averageThroughput := count / lastCompletion
	averageTurnaround := totalTurnaround / count

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, averageWait, averageTurnaround, averageThroughput)
}

//endregion

// Checkers for RR function
func containsPID(schedule [][]string, pid int64) bool {
	for _, process := range schedule {
		if strconv.FormatInt(pid, 10) == process[0] {
			return true
		}
	}
	return false
}
func lastArrivalTime(processes []Process) int64 {
	lastArrival := int64(0)
	for _, p := range processes {
		if p.ArrivalTime > lastArrival {
			lastArrival = p.ArrivalTime
		}
	}
	return lastArrival
}

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
