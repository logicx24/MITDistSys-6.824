package mapreduce

import (
	"encoding/json"
	"sort"
	"os"
	"log"
)

type ByKey []KeyValue

func (a ByKey) Len() int {return (len(a))}

func (a ByKey) Less(i,j int) bool {return a[i].Key < a[j].Key}

func (a ByKey) Swap(i,j int) {a[i], a[j] = a[j], a[i]}

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// prepare decoder for nMap reduce files
	decoders := make([]*json.Decoder, 0, nMap)
	for i :=0;i<nMap;i++{
		fileName := reduceName(jobName, i, reduceTaskNumber)
		file, err:= os.Open(fileName)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		decoders = append(decoders, dec)
	}

	// decode
	var kv KeyValue
	kvs := make([]KeyValue, 0)
	for _, d := range decoders{
		for d.More(){
			err := d.Decode(&kv)
			if err != nil {
				log.Fatal(err)
			}

			kvs = append(kvs, kv)
		}
	}

	// sort
	sort.Sort(ByKey(kvs))

	// merge values of the same key
	hash := make(map[string][]string)
	for _,k := range kvs{
		hash[k.Key] = append(hash[k.Key], k.Value)
	}

	// output to file
	mergeFile := mergeName(jobName, reduceTaskNumber)
	mergeF, err := os.Create(mergeFile)
	if err != nil {
		log.Fatal(err)
	}
	defer mergeF.Close()

	enc := json.NewEncoder(mergeF)
	for k, v := range hash {
		enc.Encode(KeyValue{k, reduceF(k, v)})
	}
}
