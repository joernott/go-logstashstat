// logstash statistics
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"
	 "github.com/ahmetalpbalkan/go-cursor"
)

const URL = "http://localhost:9600/_node/stats"
const TIMEOUT = 5
const SLEEPTIME = 5

type Stats struct {
	Host      string `json:"host"`
	Version   string `json:"version"`
	HTTPAddress string `json:"http_address"`
	ID string `json:"id"`
        Name string `json:"name"`
        Pipeline PipelineStats `json:pipeline`
}

type PipelineStats struct {
	Events    PipelineEventStats        `json:"events"`
	Plugins PipelinePluginStats `json:"plugins"`
}

type PipelineEventStats struct {
	DurationMillis int64 `json:"duration_in_millis"`
	In int64 `json:"in"`
	Out int64 `json:"Out"`
	Filtered int64 `json:"Filtered"`
	QueuePushDurationMillis int64 `json:"queue_push_duration_in_millis"`
}

type PipelinePluginStats struct {
        Inputs []InputStats `json:"inputs"`
        Filter []FilterStats `json:"filter"`
        output []OutputStats `json:"output"`
}

type InputStats struct {
	ID string `json:"id"`
	Events InputEventStats `json:"events"`
	Name string `json:"name"`
}

type InputEventStats struct {
	Out int64 `json:"out"`
	QueuePushDurationMillis int64 `json:"queue_push_duration_in_millis"`
}

type FilterStats struct {
	ID string `json:"id"`
	Events FilterEvents `json:"events"`
	Matches int64 `json:"matches"`
	PatternsPerField PatternStats `json:"patterns_per_field"`
	Name string `json:"name"`
}

type FilterEvents struct {
        DurationMillis int64 `json:"duration_in_millis"`
	In int64 `json:"in"`
        Out int64 `json:"out"`
}

type PatternStats struct {
	Message int64 `json:"message"`
}

type OutputStats struct {
	ID string `json:"id"`
	Events	OutputEvents `json:"events"`
	Name string `json:"name"`
}

type OutputEvents struct {
        DurationMillis int64 `json:"duration_in_millis"`
        In int64 `json:"in"`
        Out int64 `json:"out"`
}

func GetStatistics() (int, *Stats, error) {
	var result *Stats

	result = new(Stats)
	client := &http.Client{Timeout: TIMEOUT * time.Second}
	r, err := client.Get(URL)
	if err != nil {
		return 500, result, err
	}
	defer r.Body.Close()
	err = json.NewDecoder(r.Body).Decode(result)
	if err != nil {
		return 500, result, err
	}
	return r.StatusCode, result, nil

}


type Statistics struct {
	Input map[string]StatisticsEntry
	Filter map[string]StatisticsEntry
	Output map[string]StatisticsEntry
	Overall StatisticsEntry
}

type StatisticsEntry struct {
	CurrentIn int64
	OldIn int64
	CurrentOut int64
	OldOut int64
	CurrentDuration int64
	OldDuration int64
	Modified bool
}

func (s Statistics)Update(Data *Stats) {
	var x StatisticsEntry
	var ok bool
	for _,i:=range s.Input {
		i.Modified=false
	}
	for _,I:=range Data.Pipeline.Plugins.Inputs {
		if x,ok=s.Input[I.ID]; ok {
			x.OldOut=x.CurrentOut
			x.CurrentOut=I.Events.Out
			x.Modified=true
		} else {
			x.OldOut=0
			x.CurrentOut=I.Events.Out
			x.Modified=true
		}
		s.Input[I.ID]=x
	}
}


func (s Statistics)Print () {
    var keys []string
    
    inputkeys=make([]string,len(s))
    i:=0
    for k,_ := range s {
        inputkeys[i]=k
        k++
    }
    sort.Strings(inputkeys)
    //fmt.Print(cursor.ClearEntireScreen())
    fmt.Print(cursor.MoveTo(0, 0))
    fmt.Println("Mod Id                                    :        Out (Delta)")
    for id:0;i<len(inputkeys); id++ {
        i:=s[id]
	    mod:=" "
	if i.Modified {
	   mod="*"
        }
	x:=id
	if len(x)>40 {
		x=x[(len(x)-40):len(x)]
	}
	fmt.Printf("%v %v: %8d (%+5d)\n",mod,x,i.CurrentOut,i.CurrentOut-i.OldOut)
    }

}

func NewStatistics() (Statistics) {
	var s Statistics
	s.Input=make(map[string]StatisticsEntry)
	s.Filter=make(map[string]StatisticsEntry)
	s.Output=make(map[string]StatisticsEntry)
	return s  
}


func main() {
	var statistics Statistics
	statistics=NewStatistics()
	for {
		Status, result, err := GetStatistics()
		if err != nil {
			fmt.Println(err)
		} else {
			if Status == http.StatusOK {
				statistics.Update(result)
				statistics.Print()
			} else {
				fmt.Println("HTTP Status != 200")
			}
		}
		time.Sleep(SLEEPTIME*time.Second)
	}
}
