package bitstar

import "go.uber.org/zap"

type BadgerZapAdapter struct {
	Sugar *zap.SugaredLogger
}

func (b *BadgerZapAdapter) Errorf(msg string, args ...interface{}) {
	b.Sugar.Errorf(msg, args...)
}

func (b *BadgerZapAdapter) Warningf(msg string, args ...interface{}) {
	b.Sugar.Warnf(msg, args...)
}

func (b *BadgerZapAdapter) Infof(msg string, args ...interface{}) {
	b.Sugar.Infof(msg, args...)
}

func (b *BadgerZapAdapter) Debugf(msg string, args ...interface{}) {
	b.Sugar.Debugf(msg, args...)
}
