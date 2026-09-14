package repository

import "context"

type Transaction interface {
	WithTx(context.Context, func(context.Context) error) error
}
