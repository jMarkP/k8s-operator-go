/*
Copyright 2021.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	"errors"
	"fmt"
	"regexp"

	batchv1beta1 "k8s.io/api/batch/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// TimezoneSpec defines the timezone which governs scheduled times
type TimezoneSpec struct {
	// Name of the timezone in the tzData. See https://golang.org/pkg/time/#LoadLocation for possible values
	Name string `json:"name"`

	// Additional offset from the specified timezone
	// +optional
	OffsetSeconds int32 `json:"offset"`
}

type EventType string

const (
	EventTypeStart   EventType = "start"
	EventTypeStop    EventType = "stop"
	EventTypeRestart EventType = "restart"
)

// FriendlyScheduleSpec is a more user friendly way to specify an event schedule
// It's more limited than the format supported by CronSchedule
type FriendlyScheduleSpec struct {
	// TimeOfDay this event happens on the specified days
	// Format: hh:mm
	// +kubebuilder:validation:Pattern:=`^(\d{2}):(\d{2})$`
	TimeOfDay string `json:"timeOfDay"`

	// DaysOfWeek this event occurs on.
	// Either a comma separated list (MON,TUE,THU)
	// Or a range (MON-FRI)
	// +kubebuilder:validation:Pattern:=`(?:^([a-zA-Z]{3})(?:,([a-zA-Z]{3}))?(?:,([a-zA-Z]{3}))?(?:,([a-zA-Z]{3}))?(?:,([a-zA-Z]{3}))?(?:,([a-zA-Z]{3}))?(?:,([a-zA-Z]{3}))?)$|^(?P<startRange>[a-zA-Z]{3})-(?P<endRange>[a-zA-Z]{3}$)`
	DaysOfWeek string `json:"daysOfWeek"`
}

// A specific event in the schedule
type EventSpec struct {
	// Action to take at the specified time(s)
	Action EventType `json:"action"`

	// CronSchedule can contain an arbitrary Golang Cron Schedule
	// (see https://pkg.go.dev/github.com/robfig/cron#hdr-CRON_Expression_Format)
	// If set, takes precedence over Schedule
	// +optional
	CronSchedule string `json:"cronSchedule,omitempty"`

	// Schedule is a more user friendly way to specify an event schedule
	// It's more limited than the format supported by CronSchedule
	Schedule FriendlyScheduleSpec `json:"schedule,omitempty"`
}

var (
	timeOfDayRegex  = regexp.MustCompile(`^(\d{2}):(\d{2})$`)
	daysOfWeekRegex = regexp.MustCompile(`(?:^([a-zA-Z]{3})(?:,([a-zA-Z]{3}))?(?:,([a-zA-Z]{3}))?(?:,([a-zA-Z]{3}))?(?:,([a-zA-Z]{3}))?(?:,([a-zA-Z]{3}))?(?:,([a-zA-Z]{3}))?)$|^(?P<startRange>[a-zA-Z]{3})-(?P<endRange>[a-zA-Z]{3}$)`)
)

func (e *EventSpec) AsCronSpec() (string, error) {
	if e.CronSchedule != "" {
		return e.CronSchedule, nil
	}
	if e.Schedule.TimeOfDay == "" || e.Schedule.DaysOfWeek == "" {
		return "", errors.New("Must specify either cronSchedule or schedule")
	}
	timeOfDayMatches := timeOfDayRegex.FindStringSubmatch(e.Schedule.TimeOfDay)
	if len(timeOfDayMatches) != 3 {
		return "", errors.New("daysOfWeek must be in the format MON-FRI or SAT,SUN,TUE,WED")
	}

	if !daysOfWeekRegex.Match([]byte(e.Schedule.DaysOfWeek)) {
		return "", errors.New("timeOfDay must be in the format hh:mm")
	}

	return fmt.Sprintf("0 %s %s * * %s", timeOfDayMatches[2], timeOfDayMatches[1], e.Schedule.DaysOfWeek), nil
}

// ConcurrencyPolicy describes how the job will be handled.
// Only one of the following concurrent policies may be specified.
// If none of the following policies is specified, the default one
// is AllowConcurrent.
// +kubebuilder:validation:Enum=Allow;Forbid;Replace
type ConcurrencyPolicy string

const (
	// AllowConcurrent allows CronJobs to run concurrently.
	AllowConcurrent ConcurrencyPolicy = "Allow"

	// ForbidConcurrent forbids concurrent runs, skipping next run if previous
	// hasn't finished yet.
	ForbidConcurrent ConcurrencyPolicy = "Forbid"

	// ReplaceConcurrent cancels currently running job and replaces it with a new one.
	ReplaceConcurrent ConcurrencyPolicy = "Replace"
)

// ControlledJobSpec defines the desired state of ControlledJob
type ControlledJobSpec struct {
	// Important: Run "make" to regenerate code after modifying this file

	// Timezone which governs all specified schedule times
	Timezone TimezoneSpec `json:"timezone"`

	// Events to control the job
	Events []EventSpec `json:"events"`

	// Specifies the job that will be created when executing a CronJob.
	JobTemplate batchv1beta1.JobTemplateSpec `json:"jobTemplate"`

	//+kubebuilder:validation:Minimum=0

	// Optional deadline in seconds for starting the job if it misses scheduled
	// time for any reason.  Missed jobs executions will be counted as failed ones.
	// +optional
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty"`

	// Specifies how to treat concurrent executions of a Job.
	// Valid values are:
	// - "Allow" (default): allows CronJobs to run concurrently;
	// - "Forbid": forbids concurrent runs, skipping next run if previous run hasn't finished yet;
	// - "Replace": cancels currently running job and replaces it with a new one
	// +optional
	ConcurrencyPolicy ConcurrencyPolicy `json:"concurrencyPolicy,omitempty"`

	// This flag tells the controller to suspend subsequent executions, it does
	// not apply to already started executions.  Defaults to false.
	// +optional
	Suspend *bool `json:"suspend,omitempty"`

	//+kubebuilder:validation:Minimum=0

	// The number of successful finished jobs to retain.
	// This is a pointer to distinguish between explicit zero and not specified.
	// +optional
	SuccessfulJobsHistoryLimit *int32 `json:"successfulJobsHistoryLimit,omitempty"`

	//+kubebuilder:validation:Minimum=0

	// The number of failed finished jobs to retain.
	// This is a pointer to distinguish between explicit zero and not specified.
	// +optional
	FailedJobsHistoryLimit *int32 `json:"failedJobsHistoryLimit,omitempty"`
}

// ControlledJobStatus defines the observed state of ControlledJob
type ControlledJobStatus struct {
	// Important: Run "make" to regenerate code after modifying this file

	// A list of pointers to currently running jobs.
	// +optional
	Active []corev1.ObjectReference `json:"active,omitempty"`

	// Information when was the last time the job was successfully scheduled.
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// ControlledJob is the Schema for the controlledjobs API
type ControlledJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ControlledJobSpec   `json:"spec,omitempty"`
	Status ControlledJobStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// ControlledJobList contains a list of ControlledJob
type ControlledJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ControlledJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ControlledJob{}, &ControlledJobList{})
}
