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

package controllers

import (
	"context"
	"fmt"
	"sort"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	kbatch "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ref "k8s.io/client-go/tools/reference"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	batch "github.com/jmarkp/controlled-job/api/v1"
)

// ControlledJobReconciler reconciles a ControlledJob object
type ControlledJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Clock
}

/*
We'll mock out the clock to make it easier to jump around in time while testing,
the "real" clock just calls `time.Now`.
*/
type realClock struct{}

func (_ realClock) Now() time.Time { return time.Now() }

// clock knows how to get the current time.
// It can be used to fake out timing for testing.
type Clock interface {
	Now() time.Time
}

var (
	scheduledTimeAnnotation = "batch.tutorial.kubebuilder.io/scheduled-at"
	jobRunIdAnnotation      = "batch.tutorial.kubebuilder.io/job-run-id"
)

//+kubebuilder:rbac:groups=batch.jmarkp.uk,resources=controlledjobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch.jmarkp.uk,resources=controlledjobs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=batch.jmarkp.uk,resources=controlledjobs/finalizers,verbs=update
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ControlledJob object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.8.3/pkg/reconcile
func (r *ControlledJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	var controlledJob batch.ControlledJob
	if err := r.Get(ctx, req.NamespacedName, &controlledJob); err != nil {
		err = client.IgnoreNotFound(err)
		log.Error(err, "unable to fetch ControlledJob")
		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		return ctrl.Result{}, err
	}

	var childJobs kbatch.JobList
	if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace), client.MatchingFields{jobOwnerKey: req.Name}); err != nil {
		log.Error(err, "unable to list child Jobs")
		return ctrl.Result{}, err
	}

	// Split the jobs into active, successful and failed
	// And work out when the most recent one started
	activeJobs, successfulJobs, failedJobs, mostRecentTime := categoriseJobs(ctx, childJobs)

	r.recordEventStatus(ctx, &controlledJob.Status, mostRecentTime, activeJobs)
	if err := r.Status().Update(ctx, &controlledJob); err != nil {
		log.Error(err, "unable to update status")
		return ctrl.Result{}, err
	}

	////////////
	// REF
	////////////
	// var suspend = true
	// controlledJob.Spec.Suspend = &suspend
	// controlledJob.Status.IsSuspended = true
	// r.Update(ctx, &controlledJob)
	// r.Status().Update(ctx, &controlledJob)
	//return ctrl.Result{}, nil
	////////////

	log.V(1).Info("job count", "active jobs", len(activeJobs), "successful jobs", len(successfulJobs), "failed jobs", len(failedJobs))

	r.cleanupOldJobs(ctx, &controlledJob, successfulJobs, failedJobs)

	// If we're suspended we can exit now
	if controlledJob.Spec.Suspend != nil && *controlledJob.Spec.Suspend {
		log.V(1).Info("controlledjob suspended, skipping")
		return ctrl.Result{}, nil
	}

	// Work out when the next event (start or stop) is scheduled to happen
	// So that once we've actioned all current reconciliation we can sleep
	// until that time
	nextScheduledEvent, err := getNextEvent(ctx, &controlledJob, r.Now())
	if err != nil {
		log.Error(err, "unable to figure out ControlledJob schedule")
		// we don't really care about requeuing until we get an update that
		// fixes the schedule, so don't return an error
		return ctrl.Result{}, nil
	}

	/*
		We'll prep our eventual request to requeue until the next scheduled time, and then figure
		out if we actually need to take any action now
	*/
	scheduledResult := ctrl.Result{RequeueAfter: nextScheduledEvent.ScheduledTime.Sub(r.Now())} // save this so we can re-use it elsewhere
	log = log.WithValues("now", r.Now(), "next run", nextScheduledEvent.ScheduledTime)

	// HERE'S THE MAIN LOGIC OF DETERMINING WHAT ACTION TO TAKE
	//
	// A 'period' here is the time between a start event and the next stop event. We expect to be running throughout that period
	//
	// CASE 1: We're running and should be running, or stopped and should be stopped
	//   - no action
	// CASE 2: We're running and should be stopped
	//   - stop
	// CASE 3: We're stopped and should be running AND no jobs have yet been scheduled for this period
	//   - start
	// CASE 4: We're stopped and should be running AND there was a previous failed job for this period
	//  - this is a bit tricky...
	//   If the spec says to always restart then we just restart the failed job (easy)
	//   If we're told to NEVER restart then we mark ourselves suspended, in BOTH the spec (spec.Suspend) and the status (status.Suspended)
	//   If the user wants to restart the failed job, they can set the spec.Suspend flag to false
	//    - when we detect that spec.Suspend is false, but status.Suspended is true we know we need to restart
	//      the failed job. So we start a new copy of the job and clear the status.Suspended flag
	//
	// This complication is required to make sure we remain idempotent to multiple controllers running on the same
	//  ControlledJob object. Otherwise we would risk controller A resuspending the job before controller B could
	//  react to the user's unsuspend request
	// Note, we add a run_number incrementing id to each job so that we don't get a race condition and have
	//  two controllers both kick off the same job at the same time
	shouldBeRunning, startOfCurrentPeriod, err := r.calculateDesiredStatus(ctx, &controlledJob, r.Now())
	log.V(1).Info("XXX", "startOfCurrentPeriod", startOfCurrentPeriod.String())
	if err != nil {
		log.Error(err, "unable to determine running status")
		return ctrl.Result{}, err
	}
	isRunning := len(activeJobs) > 0

	controlledJob.Status.ShouldBeRunning = shouldBeRunning
	controlledJob.Status.IsRunning = isRunning
	if err := r.Status().Update(ctx, &controlledJob); err != nil {
		log.Error(err, "unable to update status")
		return ctrl.Result{}, err
	}

	// CASE 1
	if isRunning == shouldBeRunning {
		log.V(1).Info("no action to take, sleeping until next event time")
		return scheduledResult, nil
	}
	// CASE 2
	if isRunning && !shouldBeRunning {
		log.V(1).Info("job should be stopped, deleting active job(s)")
		for _, activeJob := range activeJobs {
			// we don't care if the job was already deleted
			if err := r.Delete(ctx, activeJob, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				log.Error(err, "unable to delete active job which should be stopped", "job", activeJob)
				return ctrl.Result{}, err
			}
		}
		log.V(1).Info("successfully stopped, sleeping until next event time")
		return scheduledResult, nil
	}

	// Look for existing jobs created in this period
	// This allows us to distinguish between
	// - not yet created a job for this period
	// - created a job that subsequently failed
	jobsForPeriod, err := findJobsForPeriod(ctx, childJobs, startOfCurrentPeriod)
	if err != nil {
		log.Error(err, "unable to detect existing jobs for period", "startOfCurrentPeriod", startOfCurrentPeriod)
		return ctrl.Result{}, err
	}

	var shouldStartNewJob bool
	if !isRunning && shouldBeRunning {

		// CASE 3
		if len(jobsForPeriod) == 0 {
			// No jobs yet scheduled for this period. We should start one
			log.V(1).Info("job should be running (no jobs yet created for this period)", "startOfCurrentPeriod", startOfCurrentPeriod)
			shouldStartNewJob = true
		} else {
			// CASE 4
			// There have been previous attempted runs this period.
			// Should we always restart?
			if controlledJob.Spec.FailurePolicy == batch.AlwaysRestartFailurePolicy {
				log.V(1).Info("job should be running (previous job not running - maybe failed? - and auto restart is enabled)", "startOfCurrentPeriod", startOfCurrentPeriod)
				shouldStartNewJob = true
			}
			// If we're here then:
			// - we should be running but aren't
			// - a previous job failed
			// - we're not allowed to auto restart

			// There are two possibile transitions here:
			// - the user has instructed us to unsuspend. We should unsuspend and restart the job
			// - a job has _just_ failed. We should suspend ourselves
			// ORDER HERE IS IMPORTANT. WE HAVE TO DETECT THE USER UNSUSPENDING FIRST, OR WE WILL HAVE
			//  OVERWRITTEN THAT INFORMATION
			specSuspendFlagIsSet := controlledJob.Spec.Suspend != nil && *controlledJob.Spec.Suspend
			userHasUnsuspended := !specSuspendFlagIsSet && controlledJob.Status.IsSuspended
			if userHasUnsuspended {
				log.V(1).Info("job should be running (we were suspended but the user has unsuspended us)", "startOfCurrentPeriod", startOfCurrentPeriod)
				controlledJob.Status.IsSuspended = false
				if err := r.Status().Update(ctx, &controlledJob); err != nil {
					log.Error(err, "unable to update ControlledJob status to unsuspend")
					return ctrl.Result{}, err
				}
				if err := r.Get(ctx, req.NamespacedName, &controlledJob); err != nil {
					err = client.IgnoreNotFound(err)
					log.Error(err, "unable to re-fetch ControlledJob")
					// we'll ignore not-found errors, since they can't be fixed by an immediate
					// requeue (we'll need to wait for a new notification), and we can get them
					// on deleted requests.
					return ctrl.Result{}, err
				}
				shouldStartNewJob = true
			} else {
				log.V(1).Info("job should be running (previous job not running - maybe failed? - and auto restart is disabled). Suspending ourselves.", "startOfCurrentPeriod", startOfCurrentPeriod)
				var suspend = true
				controlledJob.Spec.Suspend = &suspend
				controlledJob.Status.IsSuspended = true
				if err := r.Update(ctx, &controlledJob); err != nil {
					log.Error(err, "unable to update ControlledJob spec to suspend")
					return ctrl.Result{}, err
				}

				log.Info(fmt.Sprintf("Status: %+v", controlledJob.Status))
				if err := r.Status().Update(ctx, &controlledJob); err != nil {
					log.Error(err, "unable to update ControlledJob status")
					return ctrl.Result{}, err
				}
				shouldStartNewJob = false
			}
		}
	}

	if !shouldStartNewJob {
		log.V(1).Info("no need to start a new job, sleeping until next")
		return scheduledResult, nil
	}

	/*
		Once we've figured out what to do with existing jobs, we'll actually create our desired job
	*/

	// actually make the job...
	jobRunId := len(jobsForPeriod)
	job, err := r.constructJobForControlledJob(&controlledJob, startOfCurrentPeriod, jobRunId)
	if err != nil {
		log.Error(err, "unable to construct job from template")
		// don't bother requeuing until we get a change to the spec
		return scheduledResult, nil
	}

	// ...and create it on the cluster
	if err := r.Create(ctx, job); err != nil {
		log.Error(err, "unable to create Job for ControlledJob", "job", job)
		return ctrl.Result{}, err
	}

	log.V(1).Info("created Job for ControlledJob run", "job", job)

	// we'll requeue once we see the running job, and update our status
	return scheduledResult, nil

}

var (
	jobOwnerKey = ".metadata.controller"
	apiGVStr    = batch.GroupVersion.String()
)

func (r *ControlledJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// set up a real clock, since we're not in a test
	if r.Clock == nil {
		r.Clock = realClock{}
	}

	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &kbatch.Job{}, jobOwnerKey, func(rawObj client.Object) []string {
		// grab the job object, extract the owner...
		job := rawObj.(*kbatch.Job)
		owner := metav1.GetControllerOf(job)
		if owner == nil {
			return nil
		}
		// ...make sure it's a ControlledJob...
		if owner.APIVersion != apiGVStr || owner.Kind != "ControlledJob" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&batch.ControlledJob{}).
		Owns(&kbatch.Job{}).
		Complete(r)
}

// Work out which of the given jobs are still active, and which have failed succesfully/unsuccessfully
func categoriseJobs(ctx context.Context, childJobs kbatch.JobList) (activeJobs, successfulJobs, failedJobs []*kbatch.Job, mostRecentJobStartTime *time.Time) {
	log := log.FromContext(ctx)

	activeJobs = []*kbatch.Job{}
	successfulJobs = []*kbatch.Job{}
	failedJobs = []*kbatch.Job{}

	for i, job := range childJobs.Items {
		_, finishedType := isJobFinished(&job)
		switch finishedType {
		case "": // ongoing
			activeJobs = append(activeJobs, &childJobs.Items[i])
		case kbatch.JobFailed:
			failedJobs = append(failedJobs, &childJobs.Items[i])
		case kbatch.JobComplete:
			successfulJobs = append(successfulJobs, &childJobs.Items[i])
		}

		// We'll store the launch time in an annotation, so we'll reconstitute that from
		// the active jobs themselves.
		scheduledTimeForJob, err := getScheduledTimeForJob(&job)
		if err != nil {
			log.Error(err, "unable to parse schedule time for child job", "job", &job)
			continue
		}
		if scheduledTimeForJob != nil {
			if mostRecentJobStartTime == nil {
				mostRecentJobStartTime = scheduledTimeForJob
			} else if mostRecentJobStartTime.Before(*scheduledTimeForJob) {
				mostRecentJobStartTime = scheduledTimeForJob
			}
		}
	}
	return
}

func findJobsForPeriod(ctx context.Context, childJobs kbatch.JobList, scheduledTime time.Time) (jobsForPeriod []*kbatch.Job, err error) {
	log := log.FromContext(ctx)

	jobsForPeriod = []*kbatch.Job{}

	for _, job := range childJobs.Items {

		// We'll store the launch time in an annotation, so we'll reconstitute that from
		// the active jobs themselves.
		scheduledTimeForJob, err := getScheduledTimeForJob(&job)
		if err != nil {
			log.Error(err, "unable to parse schedule time for child job", "job", &job)
			continue
		}
		if scheduledTimeForJob != nil && *scheduledTimeForJob == scheduledTime {
			jobsForPeriod = append(jobsForPeriod, &job)
		}
	}
	return
}

// We consider a job "finished" if it has a "Complete" or "Failed" condition marked as true.
// Status conditions allow us to add extensible status information to our objects that other
// humans and controllers can examine to check things like completion and health.
func isJobFinished(job *kbatch.Job) (bool, kbatch.JobConditionType) {
	for _, c := range job.Status.Conditions {
		if (c.Type == kbatch.JobComplete || c.Type == kbatch.JobFailed) && c.Status == corev1.ConditionTrue {
			return true, c.Type
		}
	}

	return false, ""
}

// We add an annotation to jobs with their scheduled start time
func getScheduledTimeForJob(job *kbatch.Job) (*time.Time, error) {
	timeRaw := job.Annotations[scheduledTimeAnnotation]
	if len(timeRaw) == 0 {
		return nil, nil
	}

	timeParsed, err := time.Parse(time.RFC3339, timeRaw)
	if err != nil {
		return nil, err
	}
	return &timeParsed, nil
}

func (r *ControlledJobReconciler) recordEventStatus(ctx context.Context, status *batch.ControlledJobStatus, mostRecentJobStartTime *time.Time, activeJobs []*kbatch.Job) {
	log := log.FromContext(ctx)

	// Store the last event time
	if mostRecentJobStartTime != nil {
		status.LastScheduledStartTime = &metav1.Time{Time: *mostRecentJobStartTime}
	} else {
		status.LastScheduledStartTime = nil
	}

	// Record the set of currently active jobs
	status.Active = nil
	for _, activeJob := range activeJobs {
		jobRef, err := ref.GetReference(r.Scheme, activeJob)
		if err != nil {
			log.Error(err, "unable to make reference to active job", "job", activeJob)
			continue
		}
		status.Active = append(status.Active, *jobRef)
	}
	status.Active = append(status.Active, corev1.ObjectReference{Name: "foo"})
}

func (r *ControlledJobReconciler) cleanupOldJobs(ctx context.Context, controlledJob *batch.ControlledJob, successfulJobs, failedJobs []*kbatch.Job) {
	log := log.FromContext(ctx)

	// NB: deleting these is "best effort" -- if we fail on a particular one,
	// we won't requeue just to finish the deleting.
	if controlledJob.Spec.FailedJobsHistoryLimit != nil {
		sort.Slice(failedJobs, func(i, j int) bool {
			if failedJobs[i].Status.StartTime == nil {
				return failedJobs[j].Status.StartTime != nil
			}
			return failedJobs[i].Status.StartTime.Before(failedJobs[j].Status.StartTime)
		})
		for i, job := range failedJobs {
			if int32(i) >= int32(len(failedJobs))-*controlledJob.Spec.FailedJobsHistoryLimit {
				break
			}
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				log.Error(err, "unable to delete old failed job", "job", job)
			} else {
				log.V(0).Info("deleted old failed job", "job", job)
			}
		}
	}

	if controlledJob.Spec.SuccessfulJobsHistoryLimit != nil {
		sort.Slice(successfulJobs, func(i, j int) bool {
			if successfulJobs[i].Status.StartTime == nil {
				return successfulJobs[j].Status.StartTime != nil
			}
			return successfulJobs[i].Status.StartTime.Before(successfulJobs[j].Status.StartTime)
		})
		for i, job := range successfulJobs {
			if int32(i) >= int32(len(successfulJobs))-*controlledJob.Spec.SuccessfulJobsHistoryLimit {
				break
			}
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); (err) != nil {
				log.Error(err, "unable to delete old successful job", "job", job)
			} else {
				log.V(0).Info("deleted old successful job", "job", job)
			}
		}
	}
}

/*
	We need to construct a job based on our ControlledJob's template.  We'll copy over the spec
	from the template and copy some basic object meta.
	Then, we'll set the "scheduled time" annotation so that we can reconstitute our
	`LastScheduleTime` field each reconcile.
	Finally, we'll need to set an owner reference.  This allows the Kubernetes garbage collector
	to clean up jobs when we delete the ControlledJob, and allows controller-runtime to figure out
	which cronjob needs to be reconciled when a given job changes (is added, deleted, completes, etc).
*/
func (r *ControlledJobReconciler) constructJobForControlledJob(controlledJob *batch.ControlledJob, scheduledTime time.Time, jobRunId int) (*kbatch.Job, error) {
	// We want job names for a given nominal start time to have a deterministic name to avoid the same job being created twice
	name := fmt.Sprintf("%s-%d-%d", controlledJob.Name, scheduledTime.Unix(), jobRunId)

	job := &kbatch.Job{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      make(map[string]string),
			Annotations: make(map[string]string),
			Name:        name,
			Namespace:   controlledJob.Namespace,
		},
		Spec: *controlledJob.Spec.JobTemplate.Spec.DeepCopy(),
	}
	for k, v := range controlledJob.Spec.JobTemplate.Annotations {
		job.Annotations[k] = v
	}
	job.Annotations[scheduledTimeAnnotation] = scheduledTime.Format(time.RFC3339)
	job.Annotations[jobRunIdAnnotation] = fmt.Sprintf("%d", jobRunId)
	for k, v := range controlledJob.Spec.JobTemplate.Labels {
		job.Labels[k] = v
	}
	if err := ctrl.SetControllerReference(controlledJob, job, r.Scheme); err != nil {
		return nil, err
	}

	return job, nil
}
