package main

import (
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/urfave/cli"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const (
	ZERO           = "000000000000"
	CommandTimeout = time.Millisecond * 3000
	MaxLoggingNum  = 200
)

var (
	addr                 string
	clientsNum           int
	rwMode               bool
	readPro              int
	writePro             int
	keyPrefix            []byte
	roundTripTimeLogStep int64
	maxRoundTripTime     int64
	logPath              string
	keyTypes             []string
	keySpaceLen          int
	fieldSpaceLen        int
	valueSize            int
	allCommands          []*Command
	commands             []*Command
	getCommand, setCommand, incrCommand, decrCommand, sRandMemberCommand, sAddCommand, sRemCommand, zAddCommand,
	zRemCommand, zIncrByCommand, zRangeCommand, hGetCommand, hmGetCommand, hSetCommand, hmSetCommand,
	hIncrByCommand *Command

	dataChan            = make(chan Data, 100000)
	signalExit          = make(chan os.Signal, 1)
	genExit             = make(chan struct{})
	errLogChan          = make(chan string, 200000)
	slowThreshold int64 = 20
	startTime     int64
	fixedValue    []byte
	wg            sync.WaitGroup
)

func newCommands() (cs []*Command) {
	// string key
	getCommand = newCommand("GET", "string", false)
	getCommand.generate = func() {
		dataChan <- Data{getCommand, []interface{}{randKey(getCommand)}}
	}
	setCommand = newCommand("SET", "string", true)
	setCommand.generate = func() {
		dataChan <- Data{setCommand, []interface{}{randKey(setCommand), fixedValue}}
	}
	incrCommand = newCommand("INCR", "string", true)
	incrCommand.generate = func() {
		dataChan <- Data{incrCommand, []interface{}{randKeyForIntValue(incrCommand)}}
	}
	decrCommand = newCommand("DECR", "string", true)
	decrCommand.generate = func() {
		dataChan <- Data{decrCommand, []interface{}{randKeyForIntValue(decrCommand)}}
	}

	// set key
	sRandMemberCommand = newCommand("SRANDMEMBER", "set", false)
	sRandMemberCommand.generate = func() {
		dataChan <- Data{sRandMemberCommand, []interface{}{randKey(sRandMemberCommand), 1}}
	}
	sAddCommand = newCommand("SADD", "set", true)
	sAddCommand.generate = func() {
		dataChan <- Data{sAddCommand, []interface{}{randKey(sAddCommand), randField(sAddCommand)}}
	}
	sRemCommand = newCommand("SREM", "set", true)
	sRemCommand.generate = func() {
		dataChan <- Data{sRemCommand, []interface{}{randKey(sRemCommand), randField(sRemCommand)}}

	}

	// zset key
	zAddCommand = newCommand("ZADD", "zset", true)
	zAddCommand.generate = func() {
		dataChan <- Data{zAddCommand, []interface{}{randKey(zAddCommand), 100.1, randField(zAddCommand)}}
	}
	zRemCommand = newCommand("ZREM", "zset", true)
	zRemCommand.generate = func() {
		dataChan <- Data{zRemCommand, []interface{}{randKey(zRemCommand), randField(zRemCommand)}}
	}
	zIncrByCommand = newCommand("ZINCRBY", "zset", true)
	zIncrByCommand.generate = func() {
		dataChan <- Data{zIncrByCommand, []interface{}{randKey(zIncrByCommand), 10, randField(zIncrByCommand)}}
	}
	zRangeCommand = newCommand("ZRANGE", "zset", false)
	zRangeCommand.generate = func() {
		dataChan <- Data{zRangeCommand, []interface{}{randKey(zRangeCommand), 0, 10}}
	}

	// hash key
	hGetCommand = newCommand("HGET", "hash", false)
	hGetCommand.generate = func() {
		dataChan <- Data{hGetCommand, []interface{}{randKey(hGetCommand), randField(hGetCommand)}}
	}
	hmGetCommand = newCommand("HMGET", "hash", false)
	hmGetCommand.generate = func() {
		args := []interface{}{randKey(hmGetCommand)}
		for i := 0; i < 10; i++ {
			args = append(args, randField(hmGetCommand))
		}
		dataChan <- Data{hmGetCommand, args}
	}
	hSetCommand = newCommand("HSET", "hash", true)
	hSetCommand.generate = func() {
		dataChan <- Data{hSetCommand, []interface{}{randKey(hSetCommand), randField(hSetCommand), 300}}
	}
	hmSetCommand = newCommand("HMSET", "hash", true)
	hmSetCommand.generate = func() {
		args := []interface{}{randKey(hmSetCommand)}
		for i := 0; i < 10; i++ {
			args = append(args, randField(hmSetCommand), 111)
		}
		dataChan <- Data{hmSetCommand, args}
	}
	hIncrByCommand = newCommand("HINCRBY", "hash", true)
	hIncrByCommand.generate = func() {
		dataChan <- Data{hIncrByCommand, []interface{}{randKey(hIncrByCommand), randField(hIncrByCommand), 10}}
	}

	cs = append(cs, getCommand, setCommand, incrCommand, decrCommand, sRandMemberCommand, sAddCommand, sRemCommand,
		zAddCommand, zRemCommand, zIncrByCommand, zRangeCommand, hGetCommand, hmGetCommand, hSetCommand, hmSetCommand,
		hIncrByCommand)
	return
}

type Command struct {
	sync.RWMutex
	dataType         string
	write            bool
	name             string
	count            [MaxLoggingNum]int64
	total            uint64
	err              uint64
	slow             uint64
	lastKeyWriteId   int
	lastKeyReadId    int
	lastFieldWriteId int
	lastFieldReadId  int
	generate         func()
}

type Data struct {
	cmd  *Command
	args []interface{}
}

func newCommand(name string, t string, w bool) *Command {
	return &Command{name: name, dataType: t, write: w}
}

func randKey(cmd *Command) (k string) {
	var s string
	if cmd.write {
		s = strconv.Itoa(cmd.lastKeyWriteId)
		cmd.lastKeyWriteId++
		if cmd.lastKeyWriteId == keySpaceLen {
			cmd.lastKeyWriteId = 0
		}
	} else {
		s = strconv.Itoa(cmd.lastKeyReadId)
		cmd.lastKeyReadId++
		if cmd.lastKeyReadId == keySpaceLen {
			cmd.lastKeyReadId = 0
		}
	}
	if len(s) < 12 {
		k = fmt.Sprintf("%s:%s%s:%s", keyPrefix, ZERO[:12-len(s)], s, cmd.dataType)
	} else {
		k = fmt.Sprintf("%s:%s:%s", keyPrefix, s, cmd.dataType)
	}
	return
}

func randKeyForIntValue(cmd *Command) (k string) {
	var s string
	if cmd.write {
		s = strconv.Itoa(cmd.lastKeyWriteId)
		cmd.lastKeyWriteId++
		if cmd.lastKeyWriteId == keySpaceLen {
			cmd.lastKeyWriteId = 0
		}
	} else {
		s = strconv.Itoa(cmd.lastKeyReadId)
		cmd.lastKeyReadId++
		if cmd.lastKeyReadId == keySpaceLen {
			cmd.lastKeyReadId = 0
		}
	}
	if len(s) < 12 {
		k = fmt.Sprintf("%s:%s%s:%s:int", keyPrefix, ZERO[:12-len(s)], s, cmd.dataType)
	} else {
		k = fmt.Sprintf("%s:%s:%s:int", keyPrefix, s, cmd.dataType)
	}
	return
}

func randField(cmd *Command) (f string) {
	var s string
	if cmd.write {
		s = strconv.Itoa(cmd.lastFieldWriteId)
		cmd.lastFieldWriteId++
		if cmd.lastFieldWriteId == keySpaceLen {
			cmd.lastFieldWriteId = 0
		}
	} else {
		s = strconv.Itoa(cmd.lastFieldReadId)
		cmd.lastFieldReadId++
		if cmd.lastFieldReadId == keySpaceLen {
			cmd.lastFieldReadId = 0
		}
	}
	if len(s) < 12 {
		f = fmt.Sprintf("field_%s%s", ZERO[:12-len(s)], s)
	} else {
		f = fmt.Sprintf("field_%s", s)
	}
	return
}

type Generator struct {
	rfs    []func()
	wfs    []func()
	rIndex int
	wIndex int
}

func (g *Generator) write() {
	for i := 0; i < writePro; i++ {
		g.wfs[g.wIndex]()
		g.wIndex++
		if g.wIndex == len(g.wfs) {
			g.wIndex = 0
		}
	}
}

func (g *Generator) writeOnly() {
	g.wfs[g.wIndex]()
	g.wIndex++
	if g.wIndex == len(g.wfs) {
		g.wIndex = 0
	}
}

func (g *Generator) read() {
	for i := 0; i < readPro; i++ {
		g.rfs[g.rIndex]()
		g.rIndex++
		if g.rIndex == len(g.rfs) {
			g.rIndex = 0
		}
	}
}

func exit() {
	<-signalExit
	genExit <- struct{}{}
	close(dataChan)
	close(errLogChan)
}

func generateCmds() {
	defer wg.Done()
	var rfs, wfs []func()
	for _, cmd := range allCommands {
		for _, t := range keyTypes {
			if cmd.dataType == t {
				commands = append(commands, cmd)
				if cmd.write {
					wfs = append(wfs, cmd.generate)
				} else if rwMode {
					rfs = append(rfs, cmd.generate)
				}
			}
		}
	}
	gen := Generator{rfs, wfs, 0, 0}
	startTime = time.Now().UnixNano() / int64(time.Millisecond)
	if rwMode {
		for {
			select {
			default:
				gen.write()
				gen.read()
			case <-genExit:
				return
			}

		}
	} else {
		for {
			select {
			default:
				gen.writeOnly()
			case <-genExit:
				return
			}
		}
	}
}

func connectServer(addr string, num int) {
	for i := 0; i < num; i++ {
		c, err := redis.Dial("tcp", addr, redis.DialReadTimeout(CommandTimeout),
			redis.DialWriteTimeout(CommandTimeout), redis.DialConnectTimeout(time.Second))
		if err != nil {
			log.Fatalf("Could not connect: %v\n", err)
		}
		go redisDo(c.(redis.ConnWithTimeout))
	}
}

func redisDo(conn redis.ConnWithTimeout) {
	for data := range dataChan {
		start := time.Now().UnixNano() / int64(time.Millisecond)
		_, err := conn.DoWithTimeout(CommandTimeout, data.cmd.name, data.args...)
		if err == nil {
			roundTrip := time.Now().UnixNano()/int64(time.Millisecond) - start
			if roundTrip > maxRoundTripTime {
				data.cmd.Lock()
				data.cmd.count[len(data.cmd.count)-1]++
			} else {
				data.cmd.Lock()
				data.cmd.count[roundTrip/roundTripTimeLogStep]++
			}
			if roundTrip > slowThreshold {
				data.cmd.slow++
			}
		} else {
			data.cmd.Lock()
			data.cmd.err++
		}
		data.cmd.total++
		data.cmd.Unlock()
		if err != nil {
			errLogChan <- fmt.Sprintf("%s %s %s %s\n", time.Now().Format("2006-01-02 15:04:05"),
				data.cmd.name, data.args, err)
		}
	}
}

func logError() {
	defer wg.Done()
	if _, err := os.Stat(logPath); os.IsExist(err) {
		if err := os.Remove(logPath); err != nil {
			log.Fatalln("[ERROR] failed to remove file:", err)
		}
	}

	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("[ERROR] failed to open file:", err)
	}
	for s := range errLogChan {
		if _, err = f.WriteString(s); err != nil {
			log.Fatalln("[ERROR] failed to write log:", err)
		}
	}
	f.Close()
}

func summary() {
	var total, err, slow, read, write uint64
	spend := (float64(time.Now().UnixNano()/int64(time.Millisecond) - startTime)) / 1000
	fmt.Println()
	for _, c := range commands {
		sum := int64(0)
		fTotal := float64(c.total) / 100

		c.Lock()
		total += c.total
		err += c.err
		slow += c.slow
		if c.write {
			write += c.total
		} else {
			read += c.total
		}
		fmt.Println(fmt.Sprintf("====== %s ======", c.name))
		fmt.Println(fmt.Sprintf("  %d requests completed in %.2f seconds", c.total, spend))
		fmt.Println(fmt.Sprintf("  %d errors", c.err))
		fmt.Println(fmt.Sprintf("  %d slow( > 20 milliseconds) ", c.slow))
		for i, count := range c.count {
			if count == 0 {
				continue
			}
			sum += count
			if i == MaxLoggingNum-1 {
				fmt.Println(fmt.Sprintf("%.2f%% %d <= >=%d milliseconds", float64(sum)/fTotal, count,
					int64(i)*roundTripTimeLogStep))
				continue
			}
			fmt.Println(fmt.Sprintf("%.2f%% %d <= [%d-%d) milliseconds", float64(sum)/fTotal, count,
				int64(i)*roundTripTimeLogStep, (int64(i)+1)*roundTripTimeLogStep))
		}
		c.Unlock()
	}
	fmt.Println()
	fmt.Println("=================")
	fmt.Println(fmt.Sprintf("  %d requests completed in %.2f seconds", total, spend))
	fmt.Println(fmt.Sprintf("  %d errors %.2f%%", err, float64(err)*100/float64(total)))
	fmt.Println(fmt.Sprintf("  %d slow %.2f%%", slow, float64(slow)*100/float64(total)))
	fmt.Println(fmt.Sprintf("  %d parallel clients", clientsNum))
	fmt.Println(fmt.Sprintf("  %d bytes payload", valueSize))
	if rwMode {
		fmt.Println(fmt.Sprintf("  ops: %d, read[%.2f%%]: %d, write[%.2f%%]: %d", int64(float64(total)/spend),
			float64(readPro)*100/float64(readPro+writePro), read, float64(writePro)*100/float64(readPro+writePro), write))
	} else {
		fmt.Println(fmt.Sprintf("  ops: %d, read[0.00%%]: 0, write[100.00%%]: %d", int64(float64(total)/spend),
			write))
	}

}

func main() {
	cli.HelpFlag = cli.BoolFlag{
		Name:  "help",
		Usage: "Show help",
	}
	cli.VersionFlag = cli.BoolFlag{
		Name:  "version",
		Usage: "Print the version",
	}
	app := cli.NewApp()
	app.Name = "Redis Test"
	app.Usage = "redis benchmark tool"
	app.Version = "0.0.1"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "host, h",
			Value: "127.0.0.1",
			Usage: "Server hostname",
		},
		cli.IntFlag{
			Name:  "port, p",
			Value: 6379,
			Usage: "Server port",
		},
		cli.IntFlag{
			Name:  "clients, c",
			Value: 50,
			Usage: "Number of parallel connections",
		},
		cli.StringFlag{
			Name:  "keyprefix, K",
			Value: "redis_benchmark",
			Usage: "Key prefix",
		},
		cli.BoolFlag{
			Name:  "w",
			Usage: "Write only (default: read and write)",
		},
		cli.BoolFlag{
			Name:  "rw",
			Usage: "Read and write",
		},
		cli.StringFlag{
			Name:  "pro",
			Value: "1/1",
			Usage: "Proportion of read/write",
		},
		cli.StringFlag{
			Name:  "log, l",
			Value: "redis_benchmark.err.log",
			Usage: "Error log file path",
		},
		cli.Int64Flag{
			Name:  "step, s",
			Value: 10,
			Usage: "Logging step(ms) of RoundTripTime",
		},
		cli.StringSliceFlag{
			Name:  "type, t",
			Usage: "Key type (default: random in string, zset, set, hash)",
		},
		cli.IntFlag{
			Name:  "size, d",
			Value: 3,
			Usage: "Data size of SET/GET value in bytes",
		},
		cli.IntFlag{
			Name:  "keyspacelen, k",
			Value: 100000,
			Usage: `Use random keys for SET/GET/INCR/...
  Using this option the benchmark will expand the string __rand_key__
  inside an argument with a 12 digits number in the specified range
  from 0 to keyspacelen-1. The substitution changes every time a command
  is executed. Default tests use this to hit random keys in the
  specified range.`,
		},
		cli.IntFlag{
			Name:  "fieldspacelen, f",
			Value: 200,
			Usage: `Use random fields for SADD/HSET/...
  Using this option the benchmark will expand the string __rand_field__
  inside an argument with a 12 digits number in the specified range
  from 0 to fieldspacelen-1. The substitution changes every time a command
  is executed. Default tests use this to hit random fields in the
  specified range.`,
		},
	}

	app.Action = func(c *cli.Context) error {
		addr = fmt.Sprintf("%s:%d", c.String("host"), c.Int("port"))
		log.Println("[INFO] redis: ", addr)
		if c.Bool("w") {
			rwMode = false
		} else {
			rwMode = true
		}
		if rwMode {
			re := regexp.MustCompile(`^([\d]+)/([\d]+)$`)
			pro := re.FindAllStringSubmatch(c.String("pro"), -1)
			if len(pro) == 0 {
				return errors.New("[ERROR] invalid read-write proportion, e.g 2/1")
			}
			readPro, _ = strconv.Atoi(pro[0][1])
			writePro, _ = strconv.Atoi(pro[0][2])
			if readPro == 0 || writePro == 0 {
				return errors.New("[ERROR] invalid read-write proportion, e.g 2/1")
			}
		}
		if clientsNum = c.Int("clients"); clientsNum < 1 {
			return errors.New("[ERROR] invalid clients")
		}
		if valueSize = c.Int("size"); valueSize < 1 {
			return errors.New("[ERROR] invalid size")
		}
		fixedValue = make([]byte, valueSize)
		for i := 0; i < len(fixedValue); i++ {
			fixedValue[i] = 'x'
		}
		keyPrefix = []byte(c.String("keyprefix"))
		if roundTripTimeLogStep = c.Int64("step"); roundTripTimeLogStep < 1 {
			return errors.New("[ERROR] invalid log")
		}
		logPath = c.String("log")
		maxRoundTripTime = roundTripTimeLogStep * (MaxLoggingNum - 1)
		keyTypes = c.StringSlice("type")
		if keySpaceLen = c.Int("keyspacelen"); keySpaceLen < 1 {
			return errors.New("[ERROR] invalid keyspacelen")
		}
		if fieldSpaceLen = c.Int("fieldspacelen"); fieldSpaceLen < 1 {
			return errors.New("[ERROR] invalid fieldSpaceLen")
		}
		for _, k := range keyTypes {
			if k != "set" && k != "zset" && k != "hash" && k != "string" {
				return errors.New("[ERROR] invalid key type: " + k + ". Please choose from one: string, zset, set, hash")
			}
		}
		if len(keyTypes) == 0 {
			keyTypes = []string{"set", "zset", "hash", "string"}
		}
		run()
		return nil
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatalln("[ERROR]", err)
	}

}

func run() {
	signal.Notify(signalExit, syscall.SIGINT, syscall.SIGTERM)
	allCommands = newCommands()
	connectServer(addr, clientsNum)
	wg.Add(2)
	go exit()
	go logError()
	go generateCmds()
	wg.Wait()
	summary()
}
