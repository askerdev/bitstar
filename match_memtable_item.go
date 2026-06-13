package bitstar

import (
	"errors"
	"fmt"
	"slices"

	"github.com/askerdev/bitstar/filtering"
)

func matchMemTableItem(filter *filtering.Filter, item *memTableItem) (bool, error) {
	if filter == nil || filter.Expression == nil {
		return true, nil
	}

	for _, seq := range filter.Expression.Sequences {
		matched, err := matchSequence(seq, item)
		if err != nil {
			return false, err
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

func matchSequence(sequence *filtering.Sequence, item *memTableItem) (bool, error) {
	if sequence == nil {
		return true, nil
	}
	for _, factor := range sequence.Factors {
		matched, err := matchFactor(factor, item)
		if err != nil {
			return false, err
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

func matchFactor(factor *filtering.Factor, item *memTableItem) (bool, error) {
	if factor == nil {
		return true, nil
	}
	for _, term := range factor.Terms {
		matched, err := matchTerm(term, item)
		if err != nil {
			return false, err
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

func matchTerm(term *filtering.Term, item *memTableItem) (bool, error) {
	if term == nil {
		return true, nil
	}

	matched, err := matchSimple(term.Simple, item)
	if err != nil {
		return false, err
	}

	if term.Negated {
		return !matched, nil
	}
	return matched, nil
}

func matchSimple(simple *filtering.Simple, item *memTableItem) (bool, error) {
	if simple == nil {
		return true, nil
	}
	switch {
	case simple.Composite != nil:
		return matchMemTableItem(&filtering.Filter{Expression: simple.Composite}, item)
	case simple.Restriction != nil:
		return matchRestriction(simple.Restriction, item)
	}
	return true, nil
}

func matchRestriction(r *filtering.Restriction, item *memTableItem) (bool, error) {
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

		if !item.tags.TestString(tagName) {
			return false, nil
		}

		return slices.Contains(item.event.GetTags(), tagName), nil

	case "annotations":
		if len(r.Comparable.Member.Fields) == 0 || r.Arg == nil || r.Arg.Comparable == nil || r.Arg.Comparable.Member == nil || r.Arg.Comparable.Member.Value == nil {
			return false, errors.New("`annotations` requires key and value")
		}
		if !r.Arg.Comparable.Member.Value.Quoted {
			return false, errors.New("`annotations` value type mismatch, expected string, got not quoted value")
		}
		key := r.Comparable.Member.Fields[0].Value
		value := r.Arg.Comparable.Member.Value.Value

		if !item.annotations.TestString(key + "_" + value) {
			return false, nil
		}

		if item.event.GetAnnotations() != nil {
			if val, ok := item.event.GetAnnotations()[key]; ok && val == value {
				return true, nil
			}
		}
		return false, nil

	default:
		return false, fmt.Errorf("unknown field %q", fieldName)
	}
}
