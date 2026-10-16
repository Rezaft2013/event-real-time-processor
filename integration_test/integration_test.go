// integration_test.go
//go:build integration

package integration_test

import(    
    "io"
    "fmt"
    "bytes"
    "testing"
    "time"
    "strings"
    "net/http"
    "log"
    "encoding/json"
    "github.com/stretchr/testify/assert"
)

// TestE2E tests e2e event ingestion 
// Note: cluster should be up and running before running this test
func TestE2E(t *testing.T) {
	jsonData := []byte(`{    
        "type" : "first2",
        "tenant_id" : "first_tenant",
        "version" : "1.0",
        "timestamp" : 1791700437,
        "payload" : "{ \"name\" : \"reza\", \"email\" : \"test@test.com\" } "
    }`)
	
	req, _ := http.NewRequest("POST", "http://localhost:88/api/v1/events", bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Request failed: %v\n", err)
		return
	}
	defer resp.Body.Close()
    assert.NotNil(t, resp)

	body, _ := io.ReadAll(resp.Body)	
    assert.Equal(t, resp.StatusCode, http.StatusAccepted)    

    type Resp struct {
        EventID string `json:"event_id"`
    }
    var res Resp
    err = json.Unmarshal(body, &res)    
    assert.Nil(t, err)    

    time.Sleep(5*time.Second)
	getResp, _ := http.Get(fmt.Sprintf("http://localhost:88/api/v1/events/%s", res.EventID))
    defer getResp.Body.Close()

	body, _ = io.ReadAll(getResp.Body)
    assert.Equal(t, strings.Contains(string(body), "{ \"status\" : \"ok\" }"), true, "event does not exist in backend")
}