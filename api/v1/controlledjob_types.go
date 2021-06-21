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
	// If set, schedule must NOT be set
	// +optional
	CronSchedule string `json:"cronSchedule,omitempty"`

	// Schedule is a more user friendly way to specify an event schedule
	// It's more limited than the format supported by CronSchedule
	// If set, cronSchedule must NOT be set
	Schedule FriendlyScheduleSpec `json:"schedule,omitempty"`
}

// ControlledJobSpec defines the desired state of ControlledJob
type ControlledJobSpec struct {
	// Important: Run "make" to regenerate code after modifying this file

	// Timezone which governs all specified schedule times
	Timezone TimezoneSpec `json:"timezone"`

	// Events to control the job
	Events []EventSpec `json:"events"`

	// Specifies the job that will be created when executing a CronJob.
	JobTemplate batchv1beta1.JobTemplateSpec `json:"jobTemplate"`
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
