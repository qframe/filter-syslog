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

	"encoding/json"
)

const (
	version   = "0.0.0"
	pluginTyp = qtypes_constants.FILTER
	pluginPkg = "syslog"
)

type Plugin struct {
	*qtypes_plugin.Plugin
}

func New(qChan qtypes_qchannel.QChan, cfg *config.Config, name string) (p Plugin, err error) {
	p = Plugin{
		Plugin: qtypes_plugin.NewNamedPlugin(qChan, cfg, pluginTyp, pluginPkg, name, version),
	}
	return
}

// Run fetches everything from the Data channel and flushes it to stdout
func (p *Plugin) Run() {
	p.Log("notice", fmt.Sprintf("Start plugin v%s", p.Version))
	setCeeJsonKey := p.CfgStringOr("cee-json-key", "")
	setEngineNameToHost := p.CfgBoolOr("engine-name-to-host", false)
	dc := p.QChan.Data.Join()
	for {
		select {
		case val := <-dc.Read:
			switch val.(type) {
			case qtypes_messages.Message:
				qm := val.(qtypes_messages.Message)
				if qm.StopProcessing(p.Plugin, false) {
					continue
				}
				isCee := false
				if setCeeJsonKey != "" {
					if v, ok := qm.Tags[setCeeJsonKey]; !ok {
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
						kv := qm.ParseJsonMap(p.Plugin, ms, map[string]string{})
						kv["container_name"] = qm.Container.Name
						kv["container_id"] = qm.Container.ID
						kv["engine_name"] = qm.Engine.Name
						mJson, err := json.Marshal(kv)
						if err != nil {
							p.Log("error", err.Error())
							continue
						}
						qm.Tags[qtypes_syslog.KEY_MSG] = string(mJson)
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
				p.Log("trace", fmt.Sprintf("Dunno how to handle type: %s", reflect.TypeOf(val)))
			}
		}
	}
}