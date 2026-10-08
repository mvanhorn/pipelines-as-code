package wait

import (
	"testing"
	"time"

	v1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"gotest.tools/v3/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMaxConcurrency(t *testing.T) {
	base := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	at := func(sec int) *metav1.Time {
		tm := metav1.NewTime(base.Add(time.Duration(sec) * time.Second))
		return &tm
	}
	pr := func(name string, start, end *metav1.Time) v1.PipelineRun {
		out := v1.PipelineRun{}
		out.SetName(name)
		out.Status.StartTime = start
		out.Status.CompletionTime = end
		return out
	}

	tests := []struct {
		name      string
		prs       []v1.PipelineRun
		wantPeak  int
		wantNames []string
	}{
		{
			name:     "no pipelineruns at all",
			prs:      nil,
			wantPeak: 0,
		},
		{
			name:     "never started is ignored",
			prs:      []v1.PipelineRun{pr("a", nil, nil)},
			wantPeak: 0,
		},
		{
			name: "strictly sequential runs never overlap",
			prs: []v1.PipelineRun{
				pr("a", at(0), at(10)),
				pr("b", at(10), at(20)),
				pr("c", at(20), at(30)),
			},
			wantPeak:  1,
			wantNames: []string{"a"},
		},
		{
			name: "two overlapping runs are caught",
			prs: []v1.PipelineRun{
				pr("a", at(0), at(20)),
				pr("b", at(10), at(30)),
			},
			wantPeak:  2,
			wantNames: []string{"a", "b"},
		},
		{
			name: "peak is found in the middle of the window",
			prs: []v1.PipelineRun{
				pr("a", at(0), at(100)),
				pr("b", at(10), at(20)),
				pr("c", at(12), at(18)),
				pr("d", at(50), at(60)),
			},
			wantPeak:  3,
			wantNames: []string{"a", "b", "c"},
		},
		{
			name: "a run finishing within a second cannot overlap",
			prs: []v1.PipelineRun{
				pr("a", at(0), at(0)),
				pr("b", at(0), at(10)),
			},
			wantPeak:  1,
			wantNames: []string{"b"},
		},
		{
			name: "an unfinished run is assumed to run until the last end seen",
			prs: []v1.PipelineRun{
				pr("a", at(0), nil),
				pr("b", at(10), at(30)),
			},
			wantPeak:  2,
			wantNames: []string{"a", "b"},
		},
		{
			name: "out of order input is handled",
			prs: []v1.PipelineRun{
				pr("c", at(20), at(40)),
				pr("a", at(0), at(30)),
				pr("b", at(10), at(50)),
			},
			wantPeak:  3,
			wantNames: []string{"a", "b", "c"},
		},
		{
			name: "two still-running runs overlap",
			prs: []v1.PipelineRun{
				pr("a", at(0), nil),
				pr("b", at(5), nil),
			},
			wantPeak:  2,
			wantNames: []string{"a", "b"},
		},
		{
			name: "unfinished runs that start after a finished run still overlap",
			prs: []v1.PipelineRun{
				pr("a", at(0), at(10)),
				pr("b", at(12), nil),
				pr("c", at(15), nil),
			},
			wantPeak:  2,
			wantNames: []string{"b", "c"},
		},
		{
			name: "three unfinished runs all count",
			prs: []v1.PipelineRun{
				pr("a", at(0), nil),
				pr("b", at(5), nil),
				pr("c", at(10), nil),
			},
			wantPeak:  3,
			wantNames: []string{"a", "b", "c"},
		},
		{
			name: "three unfinished runs all count in reverse input order",
			prs: []v1.PipelineRun{
				pr("c", at(10), nil),
				pr("b", at(5), nil),
				pr("a", at(0), nil),
			},
			wantPeak:  3,
			wantNames: []string{"a", "b", "c"},
		},
		{
			name: "two unfinished runs sharing a start second both count",
			prs: []v1.PipelineRun{
				pr("a", at(0), nil),
				pr("b", at(0), nil),
			},
			wantPeak:  2,
			wantNames: []string{"a", "b"},
		},
		{
			name: "a single unfinished run counts",
			prs: []v1.PipelineRun{
				pr("a", at(0), nil),
			},
			wantPeak:  1,
			wantNames: []string{"a"},
		},
		{
			name: "a same-second finished point does not join an unfinished run",
			prs: []v1.PipelineRun{
				pr("a", at(0), at(0)),
				pr("b", at(0), nil),
			},
			wantPeak:  1,
			wantNames: []string{"b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MaxConcurrency(tt.prs)
			assert.Equal(t, got.Peak, tt.wantPeak)
			if tt.wantNames != nil {
				assert.DeepEqual(t, got.Names, tt.wantNames)
			}
		})
	}
}
