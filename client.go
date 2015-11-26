package main  

  
import (  
    "fmt"  
    "hash/crc32"  
    "sort"     
    "net/http"
    "encoding/json" 
    "io/ioutil"
    "os"
    "strings"
)  
   
type HashCircle []uint32  

type KeyValue struct{
    Key int `json:"key,omitempty"`
    Value string `json:"value,omitempty"`
}



func (hc HashCircle) Len() int {  
    return len(hc)  
}  
  
func (hc HashCircle) Less(i, j int) bool {  
    return hc[i] < hc[j]  
}  
  
func (hc HashCircle) Swap(i, j int) {  
    hc[i], hc[j] = hc[j], hc[i]  
}  
  
type Node struct {  
    Id       int  
    IP       string    
}  
  
func NewNode(id int, ip string) *Node {  
    return &Node{  
        Id:       id,  
        IP:       ip,  
    }  
}  
  
type ConsistentHash struct {  
    Nodes       map[uint32]Node  
    IsPresent   map[int]bool  
    HCircle      HashCircle  
    
}  
  
func NewConsistentHash() *ConsistentHash {  
    return &ConsistentHash{  
        Nodes:     make(map[uint32]Node),   
        IsPresent: make(map[int]bool),  
        HCircle:      HashCircle{},  
    }  
}  
  
func (hc *ConsistentHash) AddNode(node *Node) bool {  
 
    if _, ok := hc.IsPresent[node.Id]; ok {  
        return false  
    }  
    str := hc.ReturnNodeIP(node)  
    hc.Nodes[hc.GetHashValue(str)] = *(node)
    hc.IsPresent[node.Id] = true  
    hc.SortHashCircle()  
    return true  
}  
  
func (hc *ConsistentHash) SortHashCircle() {  
    hc.HCircle = HashCircle{}  
    for k := range hc.Nodes {  
        hc.HCircle = append(hc.HCircle, k)  
    }  
    sort.Sort(hc.HCircle)  
}  
  
func (hc *ConsistentHash) ReturnNodeIP(node *Node) string {  
    return node.IP 
}  
  
func (hc *ConsistentHash) GetHashValue(key string) uint32 {  
    return crc32.ChecksumIEEE([]byte(key))  
}  
  
func (hc *ConsistentHash) Get(key string) Node {  
    hash := hc.GetHashValue(key)  
    i := hc.SearchForNode(hash)  
    return hc.Nodes[hc.HCircle[i]]  
}  

func (hc *ConsistentHash) SearchForNode(hash uint32) int {  
    i := sort.Search(len(hc.HCircle), func(i int) bool {return hc.HCircle[i] >= hash })  
    if i < len(hc.HCircle) {  
        if i == len(hc.HCircle)-1 {  
            return 0  
        } else {  
            return i  
        }  
    } else {  
        return len(hc.HCircle) - 1  
    }  
}  
  
func PutTheKey(cHash *ConsistentHash, str string, input string){
        ipAddress := cHash.Get(str)  
        address := "http://"+ipAddress.IP+"/keys/"+str+"/"+input
		fmt.Println(address)
        req,err := http.NewRequest("PUT",address,nil)
        client := &http.Client{}
        resp, err := client.Do(req)
        if err!=nil{
            fmt.Println("Error:",err)
        }else{
            defer resp.Body.Close()
            fmt.Println("PUT Request successfully completed")
        }  
}  

func GetTheKey(key string,cHash *ConsistentHash){
    var out KeyValue 
    ipAddress:= cHash.Get(key)
	address := "http://"+ipAddress.IP+"/keys/"+key
	fmt.Println(address)
    response,err:= http.Get(address)
    if err!=nil{
        fmt.Println("Error:",err)
    }else{
        defer response.Body.Close()
        contents,err:= ioutil.ReadAll(response.Body)
        if(err!=nil){
            fmt.Println(err)
        }
        json.Unmarshal(contents,&out)
        result,_:= json.Marshal(out)
        fmt.Println(string(result))
    }
}

func GetAllKeys(address string){
     
    var out []KeyValue
    response,err:= http.Get(address)
    if err!=nil{
        fmt.Println("Error:",err)
    }else{
        defer response.Body.Close()
        contents,err:= ioutil.ReadAll(response.Body)
        if(err!=nil){
            fmt.Println(err)
        }
        json.Unmarshal(contents,&out)
        result,_:= json.Marshal(out)
        fmt.Println(string(result))
    }
}
func main() {   
    cHash := NewConsistentHash()      
    cHash.AddNode(NewNode(0, "127.0.0.1:3000"))
	cHash.AddNode(NewNode(1, "127.0.0.1:3001"))
	cHash.AddNode(NewNode(2, "127.0.0.1:3002")) 
	
	if(os.Args[1]=="PUT"){
		key := strings.Split(os.Args[2],"/")
        PutTheKey(cHash,key[0],key[1])
    } else if ((os.Args[1]=="GET") && len(os.Args)==3){
    	GetTheKey(os.Args[2],cHash)
    } else {
		GetAllKeys("http://127.0.0.1:3000/keys")
	    GetAllKeys("http://127.0.0.1:3001/keys")
	    GetAllKeys("http://127.0.0.1:3002/keys")
	}
}  