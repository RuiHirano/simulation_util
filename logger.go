package util

import (
	"log"
	"sync"
)

////////////////////////////////////////////////////////////
/////////////        Logger Class               ////////////
///////////////////////////////////////////////////////////

type Logger struct {
	Prefix string
}

func NewLogger() *Logger {
	//log.SetFlags(0)
	return &Logger{Prefix: ""}
}

func (l *Logger) SetPrefix(prefix string) {
	l.Prefix = prefix
}

func (l *Logger) Info(format string, args ...interface{}) {
	//log.SetPrefix(l.Prefix)
	//log.SetFlags(0)
	log.Printf("\x1b[32m\x1b[40m [Info] \x1b[0m"+format, args...)
}

func (l *Logger) Error(format string, args ...interface{}) {
	//log.SetPrefix(l.Prefix)
	//log.SetFlags(0)
	log.Printf("\x1b[31m\x1b[40m [Error] \x1b[0m"+format, args...)
}

func (l *Logger) Warn(format string, args ...interface{}) {
	//log.SetPrefix(l.Prefix)
	//log.SetFlags(0)
	log.Printf("\x1b[33m\x1b[40m [Warn] \x1b[0m"+format, args...)
}

func (l *Logger) Fatal(format string, args ...interface{}) {
	//log.SetPrefix(l.Prefix)
	//log.SetFlags(0)
	log.Fatalf("\x1b[31m\x1b[40m [Error] \x1b[0m"+format, args...)
}

func (l *Logger) Debug(format string, args ...interface{}) {
	//log.SetPrefix(l.Prefix)
	//log.SetFlags(0)
	log.Printf("\x1b[36m\x1b[40m [Debug] \x1b[0m"+format, args...)
}

type SyncMap struct {
	mapData *sync.Map
}

func NewMap() *SyncMap {
	return &SyncMap{mapData: new(sync.Map)}
}

// StoreMap mapにデータをストアする
func (thisMap *SyncMap) Store(key, value interface{}) {
	thisMap.mapData.Store(key, value)
}

// LoadMap mapからデータを取得する
func (thisMap *SyncMap) Load(key interface{}) (value interface{}, ok bool) {
	return thisMap.mapData.Load(key)
}

// Delete mapにあるデータを削除
func (thisMap *SyncMap) Delete(key interface{}) {
	thisMap.mapData.Delete(key)
}

////////////////////////////////////////////////////////////
/////////////               Utis               ////////////
///////////////////////////////////////////////////////////

// 配列に値があるかどうか
func Contains(s []uint64, e uint64) bool {
	for _, v := range s {
		if e == v {
			return true
		}
	}
	return false
}
