package mapreduce

import (
	"hash/fnv"
	"os"
	"encoding/json"
	"io/ioutil"
	"log"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// read in file in bytes
	bytes, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatal(err)
	}

	// map to []KeyValue
	kvs := mapF(inFile, string(bytes))

	// open json encoder beforehand
	encoders := make(map[string]*json.Encoder)
	for i :=0;i<nReduce;i++{
		rFile := reduceName(jobName, mapTaskNumber, i)
		file, err := os.OpenFile(rFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		encoders[rFile] = json.NewEncoder(file)
	}

	for _, w := range kvs {
		// select which reduce file to write to
		r := ihash(w.Key) % uint32(nReduce)
		rFile := reduceName(jobName, mapTaskNumber, int(r))
		err := encoders[rFile].Encode(&w)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
