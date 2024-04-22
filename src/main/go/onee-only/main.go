package main

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"sync"
	"syscall"
	"unsafe"
)

const (
	version = "v5"

	dataPath = "/media/oneee/Dev Storage/measurements.txt"

	pprofEnabled = true
	traceEnabled = true
)

var (
	dir = fmt.Sprintf("profiles/%s", version)

	profileTypes = []string{"goroutine", "allocs", "heap", "threadcreate", "block", "mutex"}

	numWorkers = runtime.GOMAXPROCS(0)
)

type stat struct {
	sum, max, min float64
	cnt           int
}

type entry struct {
	key  string
	stat stat
}

type remainder struct {
	loc int
	b   []byte
}

func main() {
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

	file, err := os.Open(dataPath)
	if err != nil {
		log.Fatal(err)
	}

	stat, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, int(stat.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		log.Fatal(err)
	}

	result, err := processFile(data)
	if err != nil {
		log.Fatal(err)
	}

	if err := writeResult(os.Stdout, result); err != nil {
		log.Fatal(err)
	}
}

func processFile(data []byte) (map[string]stat, error) {
	var (
		stats      = make([]map[uint64][]entry, numWorkers)
		remainders = make([]remainder, numWorkers)
		ends       = make([]int, numWorkers)
	)

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	chunkPerWorker := len(data) / numWorkers
	offset := 0

	for i := 0; i < numWorkers; i++ {
		s := chunkPerWorker
		if i == numWorkers-1 {
			s = len(data) - offset
		}
		go func(i, offset, size int) {
			defer wg.Done()
			r := bytes.NewReader(data[offset:])
			stat, rem, end, err := processChunk(r, offset, size)
			if err != nil {
				log.Fatal(err)
			}

			stats[i] = stat
			ends[i] = end
			remainders[i] = rem
		}(i, offset, s)
		offset += s
	}

	wg.Wait()

	rootStat := make(map[string]stat, len(stats)*len(stats[0]))
	for _, s := range stats {
		for _, arr := range s {
			for _, entry := range arr {
				s, ok := rootStat[entry.key]
				if !ok {
					rootStat[entry.key] = entry.stat
					continue
				}

				s.cnt += entry.stat.cnt
				s.sum += entry.stat.sum
				s.min = min(s.min, entry.stat.min)
				s.max = max(s.max, entry.stat.max)

				rootStat[entry.key] = s
			}
		}
	}

	// First remainder should always be valid.
	remainders = remainders[1:]

	for i, rem := range remainders {
		if rem.loc < ends[i] {
			continue
		}
		k, v, err := parseLine(rem.b)
		if err != nil {
			return nil, fmt.Errorf("parsing line: %w", err)
		}
		s, ok := rootStat[k]
		if !ok {
			rootStat[k] = stat{sum: v, max: v, min: v, cnt: 1}
			continue
		}

		s.cnt++
		s.sum += v
		s.max = max(s.max, v)
		s.min = min(s.min, v)
		rootStat[k] = s
	}

	return rootStat, nil
}

func processChunk(r io.Reader, offset, chunkSize int) (map[uint64][]entry, remainder, int, error) {
	sc := bufio.NewScanner(r)
	stats := make(map[uint64][]entry)

	var rem remainder
	if sc.Scan() {
		// Always send first line. Since it could be malformed data.
		rem = remainder{loc: offset, b: bytes.Clone(sc.Bytes())}
		offset += len(sc.Bytes())
	}

	limit := offset + chunkSize
	for offset < limit && sc.Scan() {
		station, val, err := parseLine(sc.Bytes())
		if err != nil {
			return nil, remainder{}, 0, fmt.Errorf("parsing line: %w", err)
		}

		hash := fnv.New64a()
		hash.Write([]byte(station))
		h := hash.Sum64()

		found := false
		for i, e := range stats[h] {
			if e.key == station {
				found = true
				e.stat.cnt++
				e.stat.sum += val
				e.stat.max = max(e.stat.max, val)
				e.stat.min = min(e.stat.min, val)
				stats[h][i] = e
				break
			}
		}
		if !found {
			stats[h] = append(stats[h], entry{
				key:  station,
				stat: stat{sum: val, max: val, min: val, cnt: 1},
			})
		}

		offset += len(sc.Bytes())
	}

	if sc.Err() != nil && sc.Err() != io.EOF {
		return nil, remainder{}, 0, fmt.Errorf("reading file: %w", sc.Err())
	}

	return stats, rem, offset, nil
}

func parseLine(line []byte) (string, float64, error) {
	idx := bytes.IndexByte(line, ';')
	station := line[:idx]
	data := line[idx+1:]

	b := make([]byte, len(station))
	copy(b, station)
	key := unsafe.String(unsafe.SliceData(b), len(b))

	val := parseFloat64(data)
	return key, val, nil
}

func parseFloat64(b []byte) float64 {
	signed := b[0] == '-'
	if signed {
		b = b[1:]
	}

	val := float64(b[len(b)-1]-'0') / 10
	mul := 1.0
	for i := len(b) - 3; i >= 0; i-- {
		val += float64(b[i]-'0') * mul
		mul *= 10
	}

	if signed {
		val = -val
	}

	return val
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

func round(n float64) float64 {
	return math.Round(n*10) / 10
}
