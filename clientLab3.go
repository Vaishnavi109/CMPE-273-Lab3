package main

import (
    "encoding/json"
    "fmt"
    "hash/crc32"
    "io/ioutil"
    "net/http"
    "os"
    "sort"
    "strconv"
    //"strings"
)
//Consistency Hashing Uses CRC32
var crc32q *crc32.Table
var ConsistencyCircle map[uint32]string
var serversList []string
//Key Value Store
type KeyValueStore struct {
    Key   int64  `json:"key"`
    Value string `json:"value"`
}

type KeysStructureStore struct {
    Keys []KeyValueStore
}


func main() {
	ConsistencyCircle = make(map[uint32]string)
	//All servers
    serversList = []string{
        "http://localhost:3000/",
        "http://localhost:3001/",
        "http://localhost:3002/"}
    
    crc32q = crc32.MakeTable(0xD5828281)


//Server selection based 
    for i := 0; i < len(serversList); i++ {
        value := crc32.Checksum([]byte(serversList[i]), crc32q)
        ConsistencyCircle[value] = serversList[i]
    }

    cmdArgs := os.Args[1:]
  
    //Client function
    Client(cmdArgs)

}


func Client(CmdArgs []string) {
	var Result interface{}
    if len(CmdArgs) == 3 {
        httpMethod := CmdArgs[0]
        keyVal, _ := strconv.ParseInt(CmdArgs[1], 10, 64)
        
        serverHostName := get(keyVal, ConsistencyCircle)
        keyStr := strconv.Itoa(int(keyVal))
        value := CmdArgs[2]

        if httpMethod == "PUT" {

            client := &http.Client{}
            fmt.Println("Server Selected=>",serverHostName)
            request, _ := http.NewRequest("PUT", serverHostName+"keys/"+keyStr+"/"+value, nil)
            response, err := client.Do(request)
            if err != nil {
                fmt.Println("Error ")
            }
            defer response.Body.Close()

            fmt.Println(response.StatusCode)
        } 

    } else if len(CmdArgs) == 2 {
        httpMethod := CmdArgs[0]
        keyVal, _ := strconv.ParseInt(CmdArgs[1], 10, 64)
        keyStr := strconv.Itoa(int(keyVal))
        serverHostName := get(keyVal, ConsistencyCircle)
        if httpMethod == "GET" {
            client := &http.Client{}
            request, _ := http.NewRequest("GET", serverHostName+"keys/"+keyStr, nil)
            response, err := client.Do(request)
            if err != nil {
                fmt.Println("Error")
            }
            defer response.Body.Close()

            body, _ := ioutil.ReadAll(response.Body)
            var Result interface{}
            _ = json.Unmarshal(body, &Result)

            var keyValue KeyValueStore
            keyyy := Result.(map[string]interface{})["key"].(float64)
            valuue := Result.(map[string]interface{})["value"].(string)

            keyValue.Key = int64(keyyy)
            keyValue.Value = valuue
            Output, _ := json.Marshal(keyValue)

            fmt.Println("\n" + string(Output) + "\n")
        } else {
            fmt.Println("Invalid HTTP Method")
        }

    } else if len(CmdArgs) == 0 {
        client := &http.Client{}
        var AllKey KeysStructureStore
        for i := 0; i < len(serversList); i++ {
            serverHostName := serversList[i]
            request, _ := http.NewRequest("GET", serverHostName+"keys", nil)
            response, err := client.Do(request)
            if err != nil {
                fmt.Println("Error")
            }
            defer response.Body.Close()

            body, _ := ioutil.ReadAll(response.Body)
            
            _ = json.Unmarshal(body, &Result)

            if Result != nil {
                keyValArr := Result.([]interface{})

                for i := 0; i < len(keyValArr); i++ {
                    var keyValue KeyValueStore
                    keyyy := keyValArr[i].(map[string]interface{})["key"].(float64)
                    valuue := keyValArr[i].(map[string]interface{})["value"].(string)

                    keyValue.Key = int64(keyyy)
                    keyValue.Value = valuue
                    AllKey.Keys = append(AllKey.Keys, keyValue)
                }
            }

        }

        Output, _ := json.Marshal(AllKey.Keys)
        fmt.Println("\n" + string(Output) + "\n")

    }

}
func get(key int64, ConsistencyCircle map[uint32]string) string {
    keyStr := strconv.Itoa(int(key))
    var Allkeys []int
    var HashValue uint32
    if len(ConsistencyCircle) == 0 {
        return ""
    } else {
        hashfunc := crc32.Checksum([]byte(keyStr), crc32q)
        
        for k := range ConsistencyCircle {
            Allkeys = append(Allkeys, int(k))
        }
        sort.Ints(Allkeys)
        
        for i := 0; i < len(Allkeys); i++ {
            if Allkeys[i] >= int(hashfunc) {
                HashValue = uint32(Allkeys[i])
                break
            }
        }
        return ConsistencyCircle[HashValue]
    }
}
