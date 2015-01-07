package main

import (
    "math"
    "fmt"
    "net"
    "strconv"
    "errors"
    "os"
    "os/exec"
    "syscall"
    "time"
    "bufio"
    "regexp"
    "encoding/json"
)

const (
    serverHost              = "0.0.0.0"
    serverPort              = 5018
    serverType              = "tcp"
    dateFormat              = "Mon Jan 2 15:04:05 2006"
    indexerBinPath          = "/usr/bin/indexer"
    configPath              = "/etc/sphinxsearch/sphinx.conf"
    logPath                 = "/var/log/sphinxindexer.log"
    searchdLogPath          = "/var/log/sphinxsearch/searchd.log"
    baseIndexerCommmand     = indexerBinPath + " --config " + configPath + " --rotate --quiet "
)

type Request struct {
    Type string
    Index string    
}

type Response struct {
    Message string
    Error string
}

var (
    searchdLogLinePrefix    = regexp.MustCompile(`\[([^\]]*)\.([0-9]{3})\s([0-9]{4})\]`)
    baseDate, _             = time.Parse(dateFormat, "Fri Sep 7 10:00:00 2012")
)

func main() {
    startListening(serverHost, serverPort)
}

func startListening(host string, port int) {    
    l, err := net.Listen(serverType, host + ":" + strconv.Itoa(port))
    if err != nil {
        fmt.Println("Error listening:", err.Error())
        os.Exit(1)        
    }
    
    defer l.Close()
    fmt.Printf("Listening on %s:%d\n", host, port)
    for {        
        conn, err := l.Accept()
        if err != nil {
            fmt.Println("Error accepting: ", err.Error())
            os.Exit(1)
        }

        go handleRequest(conn)
    }
}

func handleRequest(conn net.Conn) error {
    defer conn.Close()    
    if err := setKeepAlive(conn, 1, 3, 5); err != nil {
        fmt.Println("Could not set keep-alive parameters: ", err.Error())
    }
    buf := make([]byte, 1024)
    _, err := conn.Read(buf)
    if err != nil {
        fmt.Println("Error reading: ", err.Error())
        return err
    }

    var request Request
    err = json.Unmarshal(buf, &request)
    if err != nil {
        conn.Write(toJson(Response{"", "Could not decode the request JSON: " + err.Error()}))
        return err
    }

    if request.Type == "ping" {
        conn.Write(toJson(Response{"pong", ""}))
        return err
    }

    if request.Type != "reindex" {
        conn.Write(toJson(Response{"", "Unknown request: " + request.Type}))
        return err
    }

    err = reindex(request.Index)
    if err != nil {
        conn.Write(toJson(Response{"", "Reindexing error: " + err.Error()}))
        return err
    }

    conn.Write(toJson(Response{"OK", ""}))
    return nil
}

func toJson(v interface{}) []byte {
    js, _ := json.Marshal(v)
    return js
}

func reindex(indexName string) error {
    err, lastDate := getLastTimestamp(searchdLogPath)
    if err != nil {
        return err
    }
    cmd := exec.Command(indexerBinPath, "--config " + configPath, "--rotate", "--quiet", indexName)        
    err = cmd.Run()

    return waitForRotation(searchdLogPath, "rotating index: all indexes done", lastDate)
}

func readLines(file *os.File) ([]string, error) {
    var lines []string
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        lines = append(lines, scanner.Text())
    }
    return lines, scanner.Err()
}

func getLastTimestamp(filePath string) (error, time.Time) {
    file, err := os.Open(filePath)
    if err != nil {
        return err, time.Time{}
    }
    defer file.Close()
    fileInfo, err := (*file).Stat()
    if err != nil {
        return err, time.Time{}
    }
    fileLength := fileInfo.Size()
    file.Seek(int64(-math.Min(1024, math.Max(float64(fileLength - 1), 0))), 2)
    lines, err := readLines(file)
    var lastDate time.Time

    if len(lines) == 0 {        
        lastDate = baseDate
    } else {
        last := lines[len(lines) - 1]
        lastDate, err = timeFromLog(last)
        if err != nil {
            return err, time.Time{}
        }
    }

    return nil, lastDate
}

func waitForRotation(filePath, expression string, threshold time.Time) error {
    file, err := os.Open(filePath)
    if err != nil {
        return err
    }
    defer file.Close()
    fileInfo, err := (*file).Stat()
    if err != nil {
        return err
    }
    fileLength := fileInfo.Size()
    if err != nil {
        return err
    }
    _, err = file.Seek(int64(-math.Min(1024, math.Max(0, float64(fileLength - 1)))), 2)
    if err != nil {
        return err
    }

    re := regexp.MustCompile(`\[([^\]]*)\.([0-9]{3})\s([0-9]{4})\].*` + expression)
    for loop := 0; loop < 10; loop++ {
        _, _ = file.Seek(0, 1)
        scanner := bufio.NewScanner(file)
        scanner.Scan()
        if line := scanner.Text(); line != "" {
            fmt.Println(line)
            match := re.FindStringSubmatch(line)
            if len(match) > 1 {
                date, err := timeFromLog(line)
                if err != nil {
                    return err
                }
                if date.After(threshold) {
                    break
                }
            }
        }
    }

    return nil
}

func timeFromLog(line string) (time.Time, error) {
    match := searchdLogLinePrefix.FindStringSubmatch(line)
    if len(match) > 1 {
        dateStr := match[1]
        milliseconds := match[2]
        year := match[3]
        date, _ := time.Parse(dateFormat, dateStr + " " + year)
        date.Add(time.Millisecond * time.Duration(atoi(milliseconds)))
        return date, nil
    }

    return time.Time{}, errors.New("Could not match a timestamp prefix")
}

func atoi(value string) (ret int) {
    ret, _ = strconv.Atoi(value)
    return
}

func setKeepAlive(connection net.Conn, afterIdleSecs, intervalSecs, maxFails int) error {
    tcp, ok := connection.(*net.TCPConn)
    if !ok {
        return fmt.Errorf("Unsupported connection type: %T", connection)
    }
    if err := tcp.SetKeepAlive(true); err != nil {
        return err
    }
    file, err := tcp.File()
    if err != nil {
        return err
    }

    fd := int(file.Fd())
    if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE, afterIdleSecs); err != nil {
        return err
    }
    if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL, intervalSecs); err != nil {
        return err
    }
    
    return syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT, maxFails)
}