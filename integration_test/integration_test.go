// integration_test.go
//go:build integration

package integration_test

import(
    "fmt"
    "io"
    "bytes"
    "testing"
    "time"
    "strings"
    "net/http"
    "github.com/stretchr/testify/assert"
)

func TestE2E(t *testing.T) {
	jsonData := []byte(`{
    "id" : "629d06f2-2df9-4d67-af21-70aa4b88ad56",
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
		fmt.Printf("Request failed: %v\n", err)
		return
	}
	defer resp.Body.Close()
    assert.NotNil(t, resp)

	body, _ := io.ReadAll(resp.Body)	
    fmt.Println(string(body))
    assert.Equal(t, strings.Contains(string(body), "event received"), true, "failed to create the event")

    time.Sleep(5*time.Second)
	getResp, _ := http.Get("http://localhost:88/api/v1/events/629d06f2-2df9-4d67-af21-70aa4b88ad56")	
    defer getResp.Body.Close()

	body, _ = io.ReadAll(getResp.Body)
    assert.Equal(t, strings.Contains(string(body), "{ \"status\" : \"ok\" }"), true, "event does not exist in backend")
}