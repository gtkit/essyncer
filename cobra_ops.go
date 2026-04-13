package essyncer

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type CobraOpsOptions struct {
	Use    string
	Short  string
	Syncer *Syncer
	Models map[string]Syncable
}

func NewOpsCommand(opts CobraOpsOptions) *cobra.Command {
	use := opts.Use
	if use == "" {
		use = "essyncer"
	}
	short := opts.Short
	if short == "" {
		short = "Operate essyncer maintenance workflows"
	}

	cmd := &cobra.Command{
		Use:   use,
		Short: short,
	}
	cmd.AddCommand(
		newFullSyncCommand(opts),
		newDocCommand(opts),
		newOutboxCommand(opts),
		newRelayCommand(opts),
		newReconcileCommand(opts),
	)
	return cmd
}

func newDocCommand(opts CobraOpsOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "doc",
		Short: "Operate single-document repair workflows",
	}
	cmd.AddCommand(
		newDocCreateCommand(opts),
		newDocUpdateCommand(opts),
		newDocDeleteCommand(opts),
		newDocGetCommand(opts),
	)
	return cmd
}

func newFullSyncCommand(opts CobraOpsOptions) *cobra.Command {
	var checkpoint int64

	cmd := &cobra.Command{
		Use:   "full-sync [model ...]",
		Short: "Run full sync for all or selected models",
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := syncerFromOptions(opts)
			if err != nil {
				return err
			}
			models, err := resolveCommandModels(opts.Models, args)
			if err != nil {
				return err
			}

			if checkpoint != 0 {
				if len(models) != 1 {
					return errors.New("checkpoint full sync requires exactly one model")
				}
				total, err := s.FullSyncWithCheckpoint(cmd.Context(), models[0], checkpoint)
				if err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "full-sync checkpoint synced=%d\n", total)
				return nil
			}

			results := s.FullSync(cmd.Context(), models...)
			for _, result := range results {
				if result.Err != nil {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s error=%v\n", result.Table, result.Err)
					continue
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s synced=%d alias=%s\n", result.Table, result.TotalSynced, result.IndexName)
			}
			return nil
		},
	}
	cmd.Flags().Int64Var(&checkpoint, "checkpoint", 0, "start full sync from checkpoint for a single model")
	return cmd
}

func newOutboxCommand(opts CobraOpsOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "outbox",
		Short: "Inspect and maintain outbox rows",
	}
	cmd.AddCommand(
		newOutboxListCommand(opts),
		newOutboxCleanupCommand(opts),
		newOutboxReplayCommand(opts),
	)
	return cmd
}

func newDocCreateCommand(opts CobraOpsOptions) *cobra.Command {
	return newDocEnqueueCommand(opts, "create", "Read the current DB row and enqueue an index action", actionIndex)
}

func newDocUpdateCommand(opts CobraOpsOptions) *cobra.Command {
	return newDocEnqueueCommand(opts, "update", "Read the current DB row and enqueue an update action", actionUpdate)
}

func newDocEnqueueCommand(opts CobraOpsOptions, use, short string, action actionType) *cobra.Command {
	var (
		modelName string
		pk        string
		unscoped  bool
		drain     bool
	)

	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := syncerFromOptions(opts)
			if err != nil {
				return err
			}
			model, err := resolveCommandModel(opts.Models, modelName)
			if err != nil {
				return err
			}
			switch action {
			case actionIndex:
				err = s.EnqueueDocumentCreate(cmd.Context(), model, pk, unscoped)
			case actionUpdate:
				err = s.EnqueueDocumentUpdate(cmd.Context(), model, pk, unscoped)
			default:
				err = fmt.Errorf("unsupported document enqueue action %q", action)
			}
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "enqueued model=%s action=%s pk=%s\n", modelName, action, pk)
			if drain {
				result, err := s.DrainOutbox(cmd.Context(), 0)
				if err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "drain processed=%d retried=%d dead=%d remaining=%d\n", result.Processed, result.Retried, result.Dead, result.Remaining)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&modelName, "model", "", "registered model name")
	cmd.Flags().StringVar(&pk, "pk", "", "single primary key value")
	cmd.Flags().BoolVar(&unscoped, "unscoped", false, "load the database row with Unscoped")
	cmd.Flags().BoolVar(&drain, "drain", false, "run relay drain after enqueueing")
	_ = cmd.MarkFlagRequired("model")
	_ = cmd.MarkFlagRequired("pk")
	return cmd
}

func newDocDeleteCommand(opts CobraOpsOptions) *cobra.Command {
	var (
		modelName string
		pk        string
		docID     string
		unscoped  bool
		drain     bool
	)

	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Enqueue a delete action for a single document",
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := syncerFromOptions(opts)
			if err != nil {
				return err
			}
			model, err := resolveCommandModel(opts.Models, modelName)
			if err != nil {
				return err
			}
			if pk == "" && docID == "" {
				return errors.New("either --pk or --id is required")
			}
			if err := s.EnqueueDocumentDelete(cmd.Context(), model, pk, docID, unscoped); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "enqueued model=%s action=delete id=%s pk=%s\n", modelName, docID, pk)
			if drain {
				result, err := s.DrainOutbox(cmd.Context(), 0)
				if err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "drain processed=%d retried=%d dead=%d remaining=%d\n", result.Processed, result.Retried, result.Dead, result.Remaining)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&modelName, "model", "", "registered model name")
	cmd.Flags().StringVar(&pk, "pk", "", "single primary key value")
	cmd.Flags().StringVar(&docID, "id", "", "document id to delete directly")
	cmd.Flags().BoolVar(&unscoped, "unscoped", false, "load the database row with Unscoped when resolving --pk")
	cmd.Flags().BoolVar(&drain, "drain", false, "run relay drain after enqueueing")
	_ = cmd.MarkFlagRequired("model")
	return cmd
}

func newDocGetCommand(opts CobraOpsOptions) *cobra.Command {
	var (
		modelName string
		pk        string
		docID     string
		unscoped  bool
	)

	cmd := &cobra.Command{
		Use:   "get",
		Short: "Inspect DB, ES, and outbox state for a single document",
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := syncerFromOptions(opts)
			if err != nil {
				return err
			}
			model, err := resolveCommandModel(opts.Models, modelName)
			if err != nil {
				return err
			}
			result, err := s.InspectDocument(cmd.Context(), model, pk, docID, unscoped)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(
				cmd.OutOrStdout(),
				"model=%s table=%s alias=%s doc=%s db_found=%t es_found=%t outbox_pending=%d outbox_dead=%d last_dead_error=%q\n",
				result.Model, result.Table, result.Alias, result.DocumentID,
				result.DBFound, result.ESFound, result.OutboxPending, result.OutboxDead, result.LastDeadError,
			)
			return nil
		},
	}
	cmd.Flags().StringVar(&modelName, "model", "", "registered model name")
	cmd.Flags().StringVar(&pk, "pk", "", "single primary key value")
	cmd.Flags().StringVar(&docID, "id", "", "document id")
	cmd.Flags().BoolVar(&unscoped, "unscoped", false, "load the database row with Unscoped when resolving --pk")
	_ = cmd.MarkFlagRequired("model")
	return cmd
}

func newOutboxListCommand(opts CobraOpsOptions) *cobra.Command {
	var statuses []string
	var limit int

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List outbox rows",
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := syncerFromOptions(opts)
			if err != nil {
				return err
			}
			rows, err := s.ListOutbox(cmd.Context(), OutboxListOptions{
				Statuses: statuses,
				Limit:    limit,
			})
			if err != nil {
				return err
			}
			for _, row := range rows {
				_, _ = fmt.Fprintf(
					cmd.OutOrStdout(),
					"id=%d status=%s action=%s alias=%s doc=%s attempts=%d last_error=%q\n",
					row.ID, row.Status, row.Action, row.IndexAlias, row.DocumentID, row.Attempts, row.LastError,
				)
			}
			return nil
		},
	}
	cmd.Flags().StringSliceVar(&statuses, "status", nil, "filter by outbox status")
	cmd.Flags().IntVar(&limit, "limit", 100, "max rows to return")
	return cmd
}

func newOutboxCleanupCommand(opts CobraOpsOptions) *cobra.Command {
	var statuses []string
	var olderThan time.Duration
	var limit int

	cmd := &cobra.Command{
		Use:   "cleanup",
		Short: "Delete matching outbox rows",
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := syncerFromOptions(opts)
			if err != nil {
				return err
			}
			affected, err := s.CleanupOutbox(cmd.Context(), OutboxCleanupOptions{
				Statuses:  statuses,
				OlderThan: olderThan,
				Limit:     limit,
			})
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "deleted=%d\n", affected)
			return nil
		},
	}
	cmd.Flags().StringSliceVar(&statuses, "status", []string{OutboxStatusDead}, "target statuses to delete")
	cmd.Flags().DurationVar(&olderThan, "older-than", 0, "only delete rows older than this duration")
	cmd.Flags().IntVar(&limit, "limit", 0, "max rows to delete")
	return cmd
}

func newOutboxReplayCommand(opts CobraOpsOptions) *cobra.Command {
	var ids []int64
	var allDead bool
	var limit int

	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Reset dead outbox rows back to pending",
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := syncerFromOptions(opts)
			if err != nil {
				return err
			}
			affected, err := s.ReplayOutbox(cmd.Context(), OutboxReplayOptions{
				IDs:     ids,
				AllDead: allDead,
				Limit:   limit,
			})
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "replayed=%d\n", affected)
			return nil
		},
	}
	cmd.Flags().Int64SliceVar(&ids, "id", nil, "specific outbox ids to reset")
	cmd.Flags().BoolVar(&allDead, "all-dead", false, "reset all dead rows")
	cmd.Flags().IntVar(&limit, "limit", 0, "max rows to reset")
	return cmd
}

func newRelayCommand(opts CobraOpsOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "relay",
		Short: "Operate the outbox relay",
	}
	cmd.AddCommand(newRelayDrainCommand(opts))
	return cmd
}

func newRelayDrainCommand(opts CobraOpsOptions) *cobra.Command {
	var maxBatches int

	cmd := &cobra.Command{
		Use:   "drain",
		Short: "Process outbox rows until empty or batch limit",
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := syncerFromOptions(opts)
			if err != nil {
				return err
			}
			result, err := s.DrainOutbox(cmd.Context(), maxBatches)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(
				cmd.OutOrStdout(),
				"batches=%d claimed=%d processed=%d retried=%d dead=%d remaining=%d\n",
				result.Batches, result.Claimed, result.Processed, result.Retried, result.Dead, result.Remaining,
			)
			return nil
		},
	}
	cmd.Flags().IntVar(&maxBatches, "max-batches", 0, "stop after this many batches (0 means drain until empty)")
	return cmd
}

func newReconcileCommand(opts CobraOpsOptions) *cobra.Command {
	var repair bool

	cmd := &cobra.Command{
		Use:   "reconcile [model ...]",
		Short: "Compare DB row counts against ES alias counts",
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := syncerFromOptions(opts)
			if err != nil {
				return err
			}
			models, err := resolveCommandModels(opts.Models, args)
			if err != nil {
				return err
			}
			results, err := s.ReconcileCounts(cmd.Context(), models...)
			if err != nil {
				return err
			}

			var mismatched []Syncable
			for i, result := range results {
				if result.Err != nil {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s error=%v\n", result.Model, result.Err)
					continue
				}
				_, _ = fmt.Fprintf(
					cmd.OutOrStdout(),
					"%s table=%s alias=%s db=%d es=%d match=%t\n",
					result.Model, result.Table, result.Alias, result.DBCount, result.ESCount, result.Match,
				)
				if !result.Match && len(models) > 0 {
					mismatched = append(mismatched, models[i])
				}
			}

			if repair {
				var fullSyncResults []FullSyncResult
				if len(args) == 0 {
					fullSyncResults = s.FullSync(cmd.Context())
				} else if len(mismatched) > 0 {
					fullSyncResults = s.FullSync(cmd.Context(), mismatched...)
				}
				for _, result := range fullSyncResults {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "repair %s synced=%d err=%v\n", result.Table, result.TotalSynced, result.Err)
				}
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&repair, "repair", false, "run full sync for mismatched selected models")
	return cmd
}

func syncerFromOptions(opts CobraOpsOptions) (*Syncer, error) {
	if opts.Syncer == nil {
		return nil, errors.New("essyncer cobra ops: Syncer is required")
	}
	return opts.Syncer, nil
}

func resolveCommandModel(registry map[string]Syncable, name string) (Syncable, error) {
	models, err := resolveCommandModels(registry, []string{name})
	if err != nil {
		return nil, err
	}
	return models[0], nil
}

func resolveCommandModels(registry map[string]Syncable, args []string) ([]Syncable, error) {
	if len(args) == 0 {
		return nil, nil
	}
	if len(registry) == 0 {
		return nil, errors.New("model registry is required when selecting models by name")
	}
	models := make([]Syncable, 0, len(args))
	for _, arg := range args {
		model, ok := registry[arg]
		if !ok {
			available := make([]string, 0, len(registry))
			for name := range registry {
				available = append(available, name)
			}
			slices.Sort(available)
			return nil, fmt.Errorf("unknown model %q (available: %s)", arg, strings.Join(available, ", "))
		}
		models = append(models, model)
	}
	return models, nil
}
