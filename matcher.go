package bitstar

import (
	"errors"
	"fmt"
	"slices"

	"github.com/askerdev/bitstar/filtering"
	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
)

// matchEvent проверяет, соответствует ли конкретное событие Event условиям фильтра AIP-160.
func matchEvent(filter *filtering.Filter, ev *storagepb.Event) (bool, error) {
	if filter == nil || filter.Expression == nil {
		return true, nil // Если фильтра нет, событие подходит
	}

	// Выражение (Expression) состоит из Sequences, объединенных через логическое ИЛИ (OR)
	// В вашем evalFilter: res.And(bitmap) — подождите, в стандартном AIP-160 Sequences идут через OR (пробел),
	// но судя по вашему коду `evalFilter` использует `And` для Sequences и `And` для Factors.
	// Мы будем строго следовать булевой логике вашего roaring-индекса, чтобы результаты совпадали!

	// По вашему коду evalFilter: все Sequences должны вернуть true (AND)
	for _, seq := range filter.Expression.Sequences {
		matched, err := matchSequence(seq, ev)
		if err != nil {
			return false, err
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

func matchSequence(sequence *filtering.Sequence, ev *storagepb.Event) (bool, error) {
	if sequence == nil {
		return true, nil
	}

	// По вашему коду evalSequence: все Factors должны вернуть true (AND)
	for _, factor := range sequence.Factors {
		matched, err := matchFactor(factor, ev)
		if err != nil {
			return false, err
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

func matchFactor(factor *filtering.Factor, ev *storagepb.Event) (bool, error) {
	if factor == nil {
		return true, nil
	}

	// По вашему коду evalFactor: хотя бы один Term должен вернуть true (OR)
	for _, term := range factor.Terms {
		matched, err := matchTerm(term, ev)
		if err != nil {
			return false, err
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

func matchTerm(term *filtering.Term, ev *storagepb.Event) (bool, error) {
	if term == nil {
		return true, nil
	}

	matched, err := matchSimple(term.Simple, ev)
	if err != nil {
		return false, err
	}

	if term.Negated {
		return !matched, nil
	}
	return matched, nil
}

func matchSimple(simple *filtering.Simple, ev *storagepb.Event) (bool, error) {
	if simple == nil {
		return true, nil
	}
	switch {
	case simple.Composite != nil:
		return matchEvent(&filtering.Filter{Expression: simple.Composite}, ev)
	case simple.Restriction != nil:
		return matchRestriction(simple.Restriction, ev)
	}
	return true, nil
}

func matchRestriction(r *filtering.Restriction, ev *storagepb.Event) (bool, error) {
	if r.Comparable == nil || r.Comparable.Member == nil || r.Comparable.Member.Value == nil {
		return false, errors.New("invalid restriction")
	}

	fieldName := r.Comparable.Member.Value.Value
	switch fieldName {
	case "tags":
		if r.Arg == nil || r.Arg.Comparable == nil || r.Arg.Comparable.Member == nil || r.Arg.Comparable.Member.Value == nil {
			return false, errors.New("`tags` requires argument")
		}
		if !r.Arg.Comparable.Member.Value.Quoted {
			return false, errors.New("`tags` value type mismatch, expected string, got not quoted value")
		}
		tagName := r.Arg.Comparable.Member.Value.Value

		// Проверяем наличие тега в событии (предполагается, что ev.Tags — это []string)
		return slices.Contains(ev.GetTags(), tagName), nil
	case "annotations":
		if len(r.Comparable.Member.Fields) == 0 || r.Arg == nil || r.Arg.Comparable == nil || r.Arg.Comparable.Member == nil || r.Arg.Comparable.Member.Value == nil {
			return false, errors.New("`annotations` requires key and value")
		}
		if !r.Arg.Comparable.Member.Value.Quoted {
			return false, errors.New("`annotations` value type mismatch, expected string, got not quoted value")
		}
		key := r.Comparable.Member.Fields[0].Value
		value := r.Arg.Comparable.Member.Value.Value

		// Проверяем наличие пары ключ-значение в аннотациях (предполагается, что ev.Annotations — это map[string]string)
		if ev.GetAnnotations() != nil {
			if val, ok := ev.GetAnnotations()[key]; ok && val == value {
				return true, nil
			}
		}
		return false, nil

	default:
		return false, fmt.Errorf("unknown field %q", fieldName)
	}
}
