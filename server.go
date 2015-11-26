package main
import  ("github.com/julienschmidt/httprouter"
    "fmt"
    "net/http"
    "strconv"
    "encoding/json"
    "strings"
    "sort")


type KeyValue struct{
  Key int `json:"key,omitempty"`
  Value string  `json:"value,omitempty"`
} 

var k1,k2,k3 [] KeyValue
var indexOne,indexTwo,indexThree int
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


func GetTheKeys(rw http.ResponseWriter, request *http.Request,p httprouter.Params){
  port := strings.Split(request.Host,":")
  if(port[1]=="3000"){
    sort.Sort(ByKey(k1))
    resultOne,_:= json.Marshal(k1)
    fmt.Fprintln(rw,string(resultOne))
  }else if(port[1]=="3001"){
    sort.Sort(ByKey(k2))
    resultTwo,_:= json.Marshal(k2)
    fmt.Fprintln(rw,string(resultTwo))
  }else{
    sort.Sort(ByKey(k3))
    resultThree,_:= json.Marshal(k3)
    fmt.Fprintln(rw,string(resultThree))
  }
}

func PutTheKey(rw http.ResponseWriter, request *http.Request,p httprouter.Params){
  port := strings.Split(request.Host,":")
  key,_ := strconv.Atoi(p.ByName("key_id"))
  if(port[1]=="3000"){
    k1 = append(k1,KeyValue{key,p.ByName("value")})
    indexOne++
  }else if(port[1]=="3001"){
    k2 = append(k2,KeyValue{key,p.ByName("value")})
    indexTwo++
  }else{
    k3 = append(k3,KeyValue{key,p.ByName("value")})
    indexThree++
  } 
}

func GetTheKey(rw http.ResponseWriter, request *http.Request,p httprouter.Params){ 
  op := k1
  index := indexOne
  port := strings.Split(request.Host,":")
  if(port[1]=="3001"){
    op = k2 
    index = indexTwo
  }else if(port[1]=="3002"){
    op = k3
    index = indexThree
  } 
  key,_ := strconv.Atoi(p.ByName("key_id"))
  for i:=0 ; i< index ;i++{
    if(op[i].Key==key){
      result,_:= json.Marshal(op[i])
      fmt.Fprintln(rw,string(result))
    }
  }
}



func main(){
  indexOne = 0
  indexTwo = 0
  indexThree = 0
  mux := httprouter.New()
    mux.GET("/keys",GetTheKeys)
    mux.GET("/keys/:key_id",GetTheKey)
    mux.PUT("/keys/:key_id/:value",PutTheKey)
    go http.ListenAndServe(":3000",mux)
    go http.ListenAndServe(":3001",mux)
    go http.ListenAndServe(":3002",mux)
    select {}
}