package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"strconv"
)

const (
	version = "v1"

	dataPath = "/media/oneee/Dev Storage/measurements.txt"

	pprofEnabled = true
	traceEnabled = true
)

var (
	dir = fmt.Sprintf("profiles/%s", version)

	profileTypes = []string{"goroutine", "allocs", "heap", "threadcreate", "block", "mutex"}

	// numWorkers = runtime.GOMAXPROCS(0)
)

type stat struct {
	sum, max, min float64
	cnt           int
}

func main() {
	file, size, err := openFile(dataPath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	if pprofEnabled {
		os.MkdirAll(dir, 0755)

		for _, profileType := range profileTypes {
			path := filepath.Join(dir, profileType+".pprof")

			file, err := os.Create(path)
			if err != nil {
				log.Fatal(err)
			}

			defer file.Close()
			defer pprof.Lookup(profileType).WriteTo(file, 0)
		}

		file, err := os.Create(filepath.Join(dir, "cpu.pprof"))
		if err != nil {
			log.Fatal(err)
		}

		defer file.Close()
		pprof.StartCPUProfile(file)
		defer pprof.StopCPUProfile()
	}

	if traceEnabled {
		os.MkdirAll(dir, 0755)
		path := filepath.Join(dir, "trace.out")

		file, err := os.Create(path)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		if err := trace.Start(file); err != nil {
			log.Fatal(err)
		}
		defer trace.Stop()
	}

	result, err := processFile(file, size)
	if err != nil {
		log.Fatal(err)
	}

	if err := writeResult(os.Stdout, result); err != nil {
		log.Fatal(err)
	}
}

func processFile(file *os.File, size int64) (map[string]stat, error) {
	sc := bufio.NewScanner(file)

	stats := make(map[string]stat)
	for sc.Scan() {
		key, val, err := parseLine(sc.Bytes())
		if err != nil {
			return nil, fmt.Errorf("parsing line: %w", err)
		}

		s, ok := stats[key]
		if !ok {
			stats[key] = stat{sum: val, max: val, min: val, cnt: 1}
			continue
		}

		s.cnt++
		s.max = max(s.max, val)
		s.min = min(s.min, val)
		s.sum += val

		stats[key] = s
	}

	return stats, sc.Err()
}

func parseLine(line []byte) (string, float64, error) {
	station, data, found := bytes.Cut(line, []byte(";"))
	if !found {
		return "", 0, errors.New("sep not found when splitting line")
	}

	key := string(station)
	val, err := strconv.ParseFloat(string(data), 64)
	if err != nil {
		return "", 0, fmt.Errorf("parsing data as float: %w", err)
	}

	return key, val, nil
}

func writeResult(w io.Writer, result map[string]stat) error {
	keys := make([]string, 0, len(result))
	for key := range result {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	w.Write([]byte("{"))
	for i, key := range keys {
		if i > 0 {
			w.Write([]byte(", "))
		}
		stat := result[key]

		mean := stat.sum / float64(stat.cnt)

		fmt.Fprintf(w, "%s=%.1f/%.1f/%.1f", key, round(stat.min), round(mean), round(stat.max))
	}
	w.Write([]byte("}"))

	return nil
}

func openFile(path string) (*os.File, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("opening file: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("getting file stat: %w", err)
	}

	return file, stat.Size(), nil
}

func round(n float64) float64 {
	return math.Round(n*10) / 10
}
