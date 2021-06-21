// +build !ignore_autogenerated

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

// Code generated by controller-gen. DO NOT EDIT.

package v1

import (
	corev1 "k8s.io/api/core/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ControlledJob) DeepCopyInto(out *ControlledJob) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ControlledJob.
func (in *ControlledJob) DeepCopy() *ControlledJob {
	if in == nil {
		return nil
	}
	out := new(ControlledJob)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *ControlledJob) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ControlledJobList) DeepCopyInto(out *ControlledJobList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]ControlledJob, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ControlledJobList.
func (in *ControlledJobList) DeepCopy() *ControlledJobList {
	if in == nil {
		return nil
	}
	out := new(ControlledJobList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *ControlledJobList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ControlledJobSpec) DeepCopyInto(out *ControlledJobSpec) {
	*out = *in
	out.Timezone = in.Timezone
	if in.Events != nil {
		in, out := &in.Events, &out.Events
		*out = make([]EventSpec, len(*in))
		copy(*out, *in)
	}
	in.JobTemplate.DeepCopyInto(&out.JobTemplate)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ControlledJobSpec.
func (in *ControlledJobSpec) DeepCopy() *ControlledJobSpec {
	if in == nil {
		return nil
	}
	out := new(ControlledJobSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ControlledJobStatus) DeepCopyInto(out *ControlledJobStatus) {
	*out = *in
	if in.Active != nil {
		in, out := &in.Active, &out.Active
		*out = make([]corev1.ObjectReference, len(*in))
		copy(*out, *in)
	}
	if in.LastScheduleTime != nil {
		in, out := &in.LastScheduleTime, &out.LastScheduleTime
		*out = (*in).DeepCopy()
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ControlledJobStatus.
func (in *ControlledJobStatus) DeepCopy() *ControlledJobStatus {
	if in == nil {
		return nil
	}
	out := new(ControlledJobStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *EventSpec) DeepCopyInto(out *EventSpec) {
	*out = *in
	out.Schedule = in.Schedule
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new EventSpec.
func (in *EventSpec) DeepCopy() *EventSpec {
	if in == nil {
		return nil
	}
	out := new(EventSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *FriendlyScheduleSpec) DeepCopyInto(out *FriendlyScheduleSpec) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new FriendlyScheduleSpec.
func (in *FriendlyScheduleSpec) DeepCopy() *FriendlyScheduleSpec {
	if in == nil {
		return nil
	}
	out := new(FriendlyScheduleSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *TimezoneSpec) DeepCopyInto(out *TimezoneSpec) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new TimezoneSpec.
func (in *TimezoneSpec) DeepCopy() *TimezoneSpec {
	if in == nil {
		return nil
	}
	out := new(TimezoneSpec)
	in.DeepCopyInto(out)
	return out
}
