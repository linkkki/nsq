package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/judwhite/go-svc/svc"
	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/version"
	"github.com/nsqio/nsq/nsqd"
)

type program struct {
	// 只执行一次的对象
	once sync.Once
	nsqd *nsqd.NSQD
}

func main() {
	prg := &program{}
	// INT信号：ctrl + c
	// TERM：终止。是请求彻底终止某项执行操作，它期望接收进程清除自己的状态并退出

	// Package svc可帮助自己编写Windows Service可执行文件，而不会妨碍其他目标平台
	// 运行会阻塞，直到收到sig中指定的信号之一，运行。如果sig为空，则默认使用syscall.SIGINT和syscall.SIGTERM。
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		logFatal("%s", err)
	}
}

func (p *program) Init(env svc.Environment) error {
	// 如果是在windows下运行
	if env.IsWindowsService() {
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}

func (p *program) Start() error {
	// 返回nsqd的参数默认值对象
	opts := nsqd.NewOptions()

	/*
		命令行参数读取
	*/

	// 读取用户输入的命令行配置参数
	flagSet := nsqdFlagSet(opts)
	// 必须的。来解析命令行参数写入注册的flag里
	flagSet.Parse(os.Args[1:])

	// 设置随机种子
	rand.Seed(time.Now().UTC().UnixNano())

	// Lookup：返回已经f中已注册flag的Flag结构体指针；如果flag不存在的话，返回nil。
	// Flag.Value :设置的值，为一个接口
	// flag.Getter为一个接口，Gette接口使可以取回Value接口的内容，在这里断言。这里包含了Value接口。同时多个Get() interface{}函数
	// 所有的满足Value接口的类型都同时满足Getter接口。
	// 最后断言为bool型
	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqd"))
		os.Exit(0)
	}

	/*
		将toml配置文件中的信息导入变量。
	*/

	// 配置信息，注意与Options区分
	var cfg config
	// 获取配置文件的位置
	configFile := flagSet.Lookup("config").Value.String()
	// 如果用户设置了toml配置文件
	if configFile != "" {
		// 导入配置文件到cfg变量
		_, err := toml.DecodeFile(configFile, &cfg)
		if err != nil {
			logFatal("failed to load config file %s - %s", configFile, err)
		}
	}
	// 调用cfg.Validate对配置文件的各项进行进一步的合法性检查
	cfg.Validate()

	// 将用户命令行中 和 配置文件 获取到的配置替换opts的默认项
	options.Resolve(opts, flagSet, cfg)

	/*
		初始化nsqd
	*/
	// 初始化nsqd结构体
	nsqd, err := nsqd.New(opts)
	if err != nil {
		logFatal("failed to instantiate nsqd - %s", err)
	}
	p.nsqd = nsqd

	// 元数据载入
	err = p.nsqd.LoadMetadata()
	if err != nil {
		logFatal("failed to load metadata - %s", err)
	}
	// 将当前的topic和channel信息写入nsqd.dat文件中
	err = p.nsqd.PersistMetadata()
	if err != nil {
		logFatal("failed to persist metadata - %s", err)
	}

	/*
		开始工作～～～
	*/
	go func() {
		err := p.nsqd.Main()
		if err != nil {
			p.Stop()
			os.Exit(1)
		}
	}()

	return nil
}

func (p *program) Stop() error {
	// 保证只执行一次。
	p.once.Do(func() {
		p.nsqd.Exit()
	})
	return nil
}

func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[nsqd] ", f, args...)
}
