### Questions

1. Are domain update commands triggered in non-async event handlers visible to the query model at the same time
as the domain updates performed in the transaction that triggered the event? - _It seems not, as the result of
the same query run successively after executing the command returns different results_