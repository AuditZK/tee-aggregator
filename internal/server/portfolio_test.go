package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/trackrecord/enclave/internal/auth"
	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/service"
	"go.uber.org/zap"
)

const testPortfolioID = "0123456789abcdef0123456789abcdef"

type fakePortfolioService struct {
	gotUser string
	gotID   string
	gotRef  repository.ConnectionRef
	gotName string
	err     error
}

func (f *fakePortfolioService) record(user, id string) (*repository.Portfolio, error) {
	f.gotUser, f.gotID = user, id
	if f.err != nil {
		return nil, f.err
	}
	return &repository.Portfolio{ID: testPortfolioID, Name: "A", UserUID: user}, nil
}

func (f *fakePortfolioService) Create(_ context.Context, u, name string, _ []repository.ConnectionRef) (*repository.Portfolio, error) {
	f.gotName = name
	return f.record(u, "")
}
func (f *fakePortfolioService) List(_ context.Context, u string) ([]*repository.Portfolio, error) {
	f.gotUser = u
	return []*repository.Portfolio{}, f.err
}
func (f *fakePortfolioService) Get(_ context.Context, u, id string) (*repository.Portfolio, error) {
	return f.record(u, id)
}
func (f *fakePortfolioService) Rename(_ context.Context, u, id, name string) (*repository.Portfolio, error) {
	f.gotName = name
	return f.record(u, id)
}
func (f *fakePortfolioService) Archive(_ context.Context, u, id string) error {
	_, err := f.record(u, id)
	return err
}
func (f *fakePortfolioService) AddMember(_ context.Context, u, id string, c repository.ConnectionRef) (*repository.Portfolio, error) {
	f.gotRef = c
	return f.record(u, id)
}
func (f *fakePortfolioService) RemoveMember(_ context.Context, u, id string, c repository.ConnectionRef) (*repository.Portfolio, error) {
	f.gotRef = c
	return f.record(u, id)
}

func callPortfolios(t *testing.T, fake *fakePortfolioService, uid, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	h := &Handler{logger: zap.NewNop(), portfolioSvc: fake}
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if uid != "" {
		req = req.WithContext(auth.WithUserUID(req.Context(), uid))
	}
	rec := httptest.NewRecorder()
	h.Portfolios(rec, req)
	return rec
}

// There is no request-supplied uid here at all: without a verified user, nothing.
func TestPortfolios_RefusesWithoutVerifiedUser(t *testing.T) {
	fake := &fakePortfolioService{}
	rec := callPortfolios(t, fake, "", http.MethodGet, portfoliosPath+"?user_uid=user_victim", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", rec.Code)
	}
	if fake.gotUser != "" {
		t.Fatalf("service reached with %q", fake.gotUser)
	}
}

func TestPortfolios_UserIsAlwaysTheJWTs(t *testing.T) {
	fake := &fakePortfolioService{}
	callPortfolios(t, fake, "user_attacker", http.MethodPost, portfoliosPath,
		`{"user_uid":"user_victim","name":"A","connections":[{"exchange":"okx","label":"Main"}]}`)
	if fake.gotUser != "user_attacker" {
		t.Fatalf("user = %q", fake.gotUser)
	}
}

func TestPortfolios_Routes(t *testing.T) {
	item := portfoliosPath + "/" + testPortfolioID
	cases := []struct {
		name, method, path, body string
		want                     int
	}{
		{"list", http.MethodGet, portfoliosPath, "", http.StatusOK},
		{"create", http.MethodPost, portfoliosPath, `{"name":"A"}`, http.StatusCreated},
		{"read", http.MethodGet, item, "", http.StatusOK},
		{"rename", http.MethodPatch, item, `{"name":"B"}`, http.StatusOK},
		{"archive", http.MethodDelete, item, "", http.StatusOK},
		{"add member", http.MethodPost, item + "/members", `{"exchange":"okx","label":"Main"}`, http.StatusCreated},
		{"remove member", http.MethodDelete, item + "/members", `{"exchange":"okx","label":"Main"}`, http.StatusOK},
		{"malformed id", http.MethodGet, portfoliosPath + "/../secrets", "", http.StatusNotFound},
		{"unknown sub-path", http.MethodGet, item + "/other", "", http.StatusNotFound},
		{"wrong method", http.MethodPut, portfoliosPath, "", http.StatusMethodNotAllowed},
		{"wrong method on members", http.MethodGet, item + "/members", "", http.StatusMethodNotAllowed},
		{"bad body", http.MethodPost, portfoliosPath, `{`, http.StatusBadRequest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := callPortfolios(t, &fakePortfolioService{}, "user_a", c.method, c.path, c.body)
			if rec.Code != c.want {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, c.want, rec.Body.String())
			}
		})
	}
}

func TestPortfolios_MembersReachTheService(t *testing.T) {
	fake := &fakePortfolioService{}
	callPortfolios(t, fake, "user_a", http.MethodDelete, portfoliosPath+"/"+testPortfolioID+"/members",
		`{"exchange":"okx","label":"Main"}`)
	if fake.gotID != testPortfolioID || fake.gotRef.Exchange != "okx" || fake.gotRef.Label != "Main" {
		t.Fatalf("got id=%q ref=%+v", fake.gotID, fake.gotRef)
	}
}

func TestPortfolios_ErrorStatuses(t *testing.T) {
	cases := []struct {
		err  error
		want int
	}{
		{repository.ErrNotFound, http.StatusNotFound},
		{service.ErrPortfolioInvalid, http.StatusBadRequest},
		{service.ErrPortfolioConnectionRefused, http.StatusConflict},
		{repository.ErrPortfolioNameTaken, http.StatusConflict},
		{repository.ErrPortfolioMemberExists, http.StatusConflict},
		{repository.ErrPortfoliosUnavailable, http.StatusServiceUnavailable},
		{errors.New("pq: secret database detail"), http.StatusInternalServerError},
	}
	for _, c := range cases {
		rec := callPortfolios(t, &fakePortfolioService{err: c.err}, "user_a", http.MethodGet, portfoliosPath, "")
		if rec.Code != c.want {
			t.Errorf("%v: status = %d, want %d", c.err, rec.Code, c.want)
		}
		var body map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		if strings.Contains(rec.Body.String(), "secret database detail") {
			t.Errorf("leaked: %s", rec.Body.String())
		}
	}
}

func TestPortfolios_NotConfigured(t *testing.T) {
	h := &Handler{logger: zap.NewNop()}
	req := httptest.NewRequest(http.MethodGet, portfoliosPath, nil)
	req = req.WithContext(auth.WithUserUID(req.Context(), "user_a"))
	rec := httptest.NewRecorder()
	h.Portfolios(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", rec.Code)
	}
}
