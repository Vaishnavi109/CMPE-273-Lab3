
package main

import (
    "encoding/json"
    "fmt"
    "github.com/julienschmidt/httprouter"
    "net/http"
    "strconv"
    //"strings"
)

//structure for all keys
var AllKeys KeysStructureStore
//key value Store
type KeyValueStore struct {
    Key   int64  `json:"key"`
    Value string `json:"value"`
}

type KeysStructureStore struct {
    Keys []KeyValueStore
}
//function to Put Data
func PutData(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
    var keyValue KeyValueStore
    key_idStr := p.ByName("key_id")
    key_id, _ := strconv.ParseInt(key_idStr, 10, 64)
    value := p.ByName("value")

    
    keyValue.Key = key_id
    keyValue.Value = value

    AllKeys.Keys = append(AllKeys.Keys, keyValue)

    fw, _ := json.Marshal(&keyValue)
    fmt.Fprintf(rw, string(fw))

    fw, _ = json.Marshal(&AllKeys.Keys)
    fmt.Println(string(fw))

}


//start 3 servers
func main() {

    mux := httprouter.New()
    mux.PUT("/keys/:key_id/:value", PutData)
    mux.GET("/keys/:key_id", GetData)
    mux.GET("/keys", getAllKeys)
    server1 := http.Server{
        Addr:    "0.0.0.0:3000",
        Handler: mux,
    }
    server2 := http.Server{
        Addr:    "0.0.0.0:3001",
        Handler: mux,
    }
    server3 := http.Server{
        Addr:    "0.0.0.0:3002",
        Handler: mux,
    }
     server1.ListenAndServe()
     server2.ListenAndServe()
     server3.ListenAndServe()
}
//funtion to get all keys
func getAllKeys(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
    if len(AllKeys.Keys) > 0 {
        Output, _ := json.Marshal(&AllKeys.Keys)
        fmt.Fprintf(rw, "\n"+string(Output)+"\n")
    }
}
//function to Get Data
func GetData(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
    key_idStr := p.ByName("key_id")
    key_id, _ := strconv.ParseInt(key_idStr, 10, 64)

    var searchKeyValue KeyValueStore
    check := false
    for i := 0; i < len(AllKeys.Keys); i++ {
        if AllKeys.Keys[i].Key == key_id {
            check = true
            searchKeyValue.Key = AllKeys.Keys[i].Key
            searchKeyValue.Value = AllKeys.Keys[i].Value
            
            break
        }
    }
    if check == true {
        Output, _ := json.Marshal(&searchKeyValue)
        fmt.Fprintf(rw, string(Output))
    } else {
        fmt.Fprintf(rw, "Not found.")
    }
}


