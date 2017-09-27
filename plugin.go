package qfilter_syslog

import (
	"fmt"
	"reflect"
	"github.com/zpatrick/go-config"

	"github.com/qframe/types/messages"
	"github.com/qframe/types/plugin"
	"github.com/qframe/types/qchannel"
	"github.com/qframe/types/constants"
	"github.com/qframe/types/syslog"
	"github.com/deckarep/golang-set"
	"sync"
)

const (
	version   = "0.0.0"
	pluginTyp = qtypes_constants.FILTER
	pluginPkg = "syslog"
)

type Plugin struct {
	*qtypes_plugin.Plugin
	mu sync.Mutex
}

func New(qChan qtypes_qchannel.QChan, cfg *config.Config, name string) (p Plugin, err error) {
	p = Plugin{
		Plugin: qtypes_plugin.NewNamedPlugin(qChan, cfg, pluginTyp, pluginPkg, name, version),
	}
	return
}


// Lock locks the plugins' mutex.
func (p *Plugin) Lock() {
	p.mu.Lock()
}

// Unlock unlocks the plugins' mutex.
func (p *Plugin) Unlock() {
	p.mu.Unlock()
}

// Run fetches everything from the Data channel and flushes it to stdout
func (p *Plugin) Run() {
	p.Log("notice", fmt.Sprintf("Start plugin v%s", p.Version))
	setCeeJsonKey := p.CfgStringOr("cee-json-key", "")
	annotateDockerMeta := p.CfgBoolOr("annotate-docker-meta", false)
	setEngineNameToHost := p.CfgBoolOr("engine-name-to-host", false)
	ignoreContainerEvents := p.CfgBoolOr("ignore-container-events", true)
	dc := p.QChan.Data.Join()
	for {
		select {
		case val := <-dc.Read:
			switch val.(type) {
			case qtypes_messages.Message:
				qm := val.(qtypes_messages.Message)
				p.Log("trace", "received qtypes_messages.Message")
				if qm.StopProcessing(p.Plugin, false) {
					continue
				}
				isCee := false
				if setCeeJsonKey != "" {
					if _, ok := qm.Tags[setCeeJsonKey]; !ok {
						p.Log("error", fmt.Sprintf("could not find 'setSyslogMsgKey' '%s' in kv", setCeeJsonKey))
					} else {
						// marshall JSON
						isCee = true
						qm.Tags[qtypes_syslog.KEY_MSG] = qm.Tags[setCeeJsonKey]
						p.Log("debug", fmt.Sprintf("Overwrite KY_MSG '%s' with '%s'", qtypes_syslog.KEY_MSG, setCeeJsonKey))
					}

				}
				sl, err := qtypes_syslog.NewSyslogFromKV(qm.Tags)
				if err != nil {
					p.Log("error", fmt.Sprintf("error parsing Tags to syslog5424 struct: %s", err.Error()))
				}
				if isCee {
					p.Log("debug", "Enable CEE prefix to message")
					sl.EnableCEE()
				}
				sm := qtypes_messages.NewSyslogMessage(qm.Base, sl)
				p.QChan.SendData(sm)
			case qtypes_messages.ContainerMessage:
				qm := val.(qtypes_messages.ContainerMessage)
				p.Log("trace", "received qtypes_messages.ContainerMessage")
				if qm.StopProcessing(p.Plugin, false) {
					continue
				}
				isCee := false
				if setCeeJsonKey != "" {
					if _, ok := qm.Tags[setCeeJsonKey]; !ok {
						p.Log("error", fmt.Sprintf("could not find 'setSyslogMsgKey' '%s' in kv", setCeeJsonKey))
					} else {
						isCee = true
						ms := mapset.NewSet(setCeeJsonKey)
						newKv := qm.Message.ParseJsonMap(p.Plugin, ms, qm.Tags)
						p.Lock()
						for k,v := range newKv {
							if oldV, ok := qm.Tags[k]; !ok {
								qm.Tags[k] = v
							} else {
								p.Log("debug", fmt.Sprintf("Won't overwrite tag '%s=%s' with val '%s'", k, oldV, v))

							}
						}
						msg := qm.Tags[setCeeJsonKey]
						// Annotate container/engine information to the JSON string
						if annotateDockerMeta {
							annoKv := map[string]string{
								"engine_name": qm.Engine.Name,
								"container_id": qm.Container.ID,
								"container_name": qm.Container.Name,
							}
							msg, _ = AnnotateMsg(msg, annoKv)
							p.Log("debug", fmt.Sprintf("Rewrote JSON string to '%s', marshaled from '%v'", msg, newKv))
						}
						p.Unlock()
						qm.Tags[qtypes_syslog.KEY_MSG] = msg
						p.Log("debug", fmt.Sprintf("Overwrite KY_MSG '%s' with '%s'", qtypes_syslog.KEY_MSG, setCeeJsonKey))
					}
				}
				sl, err := qtypes_syslog.NewSyslogFromKV(qm.Tags)
				if setEngineNameToHost {
					sl.Host = qm.Engine.Name
				}
				if err != nil {
					p.Log("error", fmt.Sprintf("error parsing Tags to syslog5424 struct: %s", err.Error()))
				}
				if isCee {
					p.Log("debug", "Enable CEE prefix to message")
					sl.EnableCEE()
				}
				sm := qtypes_messages.NewSyslogMessage(qm.Base, sl)
				p.QChan.SendData(sm)
			default:
				if ignoreContainerEvents {
					continue
				}
				p.Log("trace", fmt.Sprintf("Dunno how to handle type: %s", reflect.TypeOf(val)))
			}
		}
	}
}

// AnnotateMsg assumes the msg is a JSON string and annotates key/value pairs to the string.
func AnnotateMsg(msg string, kv map[string]string) (res string, err error) {
	res = msg[:len(msg)-1]
	for k,v := range kv {
		res = fmt.Sprintf(`%s,"%s":"%s"`, res, k,v)
	}
	return res+"}",err
}