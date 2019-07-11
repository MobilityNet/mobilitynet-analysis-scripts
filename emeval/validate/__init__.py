#
# Modules to read in the data corresponding to a spce and to populate it into 
# an in-memory data structure.
# The in-memory data structures can be either phone oriented or evaluation
# oriented. phone-oriented data is grouped by phones (e.g. ucb-sdb-android-1,
# ucb-sdb-android-2...). Evaluation-oriented data is grouped by evaluation
# (e.g. 'high_accuracy_stationary' or 'HAHFDC v/s HAMFDC'. Ideally, we would
# take the same data and make it available in both formats for maximum
# flexibility.

# Right now, this is essentially copy/pasted from the individual notebooks,
# with minor edits and restructuring to avoid duplicate code. We can explore
# the use of xarrays for this kind of multi-dimensional data in the future.

