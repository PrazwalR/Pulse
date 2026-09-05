package rules

import (
	"testing"

	"pulse/internal/model"
)

func TestMinTravelSymmetricAndUnknown(t *testing.T) {
	h1, ok1 := minTravel("Chennai", "Delhi")
	h2, ok2 := minTravel("Delhi", "Chennai")
	if !ok1 || !ok2 || h1 != h2 {
		t.Fatalf("known pair should be symmetric: %v/%v vs %v/%v", h1, ok1, h2, ok2)
	}
	if _, ok := minTravel("Chennai", "Atlantis"); ok {
		t.Fatal("unknown pair must not be known (fail toward fewer false positives)")
	}
	if h, ok := minTravel("Pune", "Pune"); !ok || h != 0 {
		t.Fatalf("same city should be 0h, got %v/%v", h, ok)
	}
}

func TestM1HighRiskMerchant(t *testing.T) {
	// fires: high-risk category + large amount (no DB read, session unused)
	got, err := M1HighRiskMerchant(nil, model.Transaction{MerchantCategory: "crypto", Amount: 9000})
	if err != nil || len(got) != 1 || got[0].Rule != "high_risk_merchant" || got[0].Severity != "medium" {
		t.Fatalf("expected one high_risk_merchant alert, got %v err=%v", got, err)
	}
	// no fire: amount below threshold
	if got, _ := M1HighRiskMerchant(nil, model.Transaction{MerchantCategory: "crypto", Amount: 100}); len(got) != 0 {
		t.Fatalf("small amount should not fire, got %v", got)
	}
	// no fire: safe category, large amount
	if got, _ := M1HighRiskMerchant(nil, model.Transaction{MerchantCategory: "groceries", Amount: 90000}); len(got) != 0 {
		t.Fatalf("safe category should not fire, got %v", got)
	}
}

func TestAllReturnsEightRules(t *testing.T) {
	if n := len(All()); n != 8 {
		t.Fatalf("expected 8 registered rules, got %d", n)
	}
}
