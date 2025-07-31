# lfdcast: Lean and Fast Data Casting

[![R build
status](https://github.com/akersting/lfdcast/R-CMD-check/badge.svg)](https://github.com/akersting/lfdcast/actions?workflow=R-CMD-check)

`lfdcast::dcast()` is similar to `data.table::dcast()` (and
`reshape2::dcast()`), while offering a more concise syntax for complex
pivoting tasks, i.e. the reshaping of data from long to wide format. At
the same time it is both performant and memory efficient, also for large
datasets with hundreds of millions of observations.

This is achieved by the following means:

- You can specify multiple sets of columns by which to simultaneously
  spread your data.
- For each of those sets you can (optionally) apply different filters to
  the data and then apply multiple aggregation functions to existing
  columns or to the results of expressions involving one or more
  columns.
- `lfdcast::dcast()` uses multi-threading for parallelization over the
  *output* columns.
- All aggregations functions are written in C.
- A lot of the heavy lifting is done by the extremely fast
  `data.table::frankv()`.

It comes with the following set of built-in aggregation functions, all
of which support the additional arguments `na.rm` and `fill`:

``` r
sort(names(lfdcast:::fun.aggregates))
```

    ##  [1] "gall"        "gany"        "gfirst"      "glast"       "glength"    
    ##  [6] "glength_gt0" "glist"       "gmax"        "gmean"       "gmedian"    
    ## [11] "gmin"        "gsample"     "gsum"        "guniqueN"

Generally, it can be extended by writing custom aggregation functions
(in C) and registering them using `lfdcast::register_fun.aggregate()`.
However, a proper documentation of the C-level API is still lacking.

## Example

This example shows how we can use `lfdcast::dcast()` to transform the
`esoph` dataset into a wide format.

First, lets have a look at the data itself:

``` r
# conver to data.table just for better printing
X <- data.table::as.data.table(datasets::esoph)
head(X, 20)
```

    ##     agegp     alcgp    tobgp ncases ncontrols
    ##     <ord>     <ord>    <ord>  <num>     <num>
    ##  1: 25-34 0-39g/day 0-9g/day      0        40
    ##  2: 25-34 0-39g/day    10-19      0        10
    ##  3: 25-34 0-39g/day    20-29      0         6
    ##  4: 25-34 0-39g/day      30+      0         5
    ##  5: 25-34     40-79 0-9g/day      0        27
    ##  6: 25-34     40-79    10-19      0         7
    ##  7: 25-34     40-79    20-29      0         4
    ##  8: 25-34     40-79      30+      0         7
    ##  9: 25-34    80-119 0-9g/day      0         2
    ## 10: 25-34    80-119    10-19      0         1
    ## 11: 25-34    80-119      30+      0         2
    ## 12: 25-34      120+ 0-9g/day      0         1
    ## 13: 25-34      120+    10-19      1         0
    ## 14: 25-34      120+    20-29      0         1
    ## 15: 25-34      120+      30+      0         2
    ## 16: 35-44 0-39g/day 0-9g/day      0        60
    ## 17: 35-44 0-39g/day    10-19      1        13
    ## 18: 35-44 0-39g/day    20-29      0         7
    ## 19: 35-44 0-39g/day      30+      0         8
    ## 20: 35-44     40-79 0-9g/day      0        35
    ##     agegp     alcgp    tobgp ncases ncontrols

Here is `?datasets::esoph`:

``` r
pkg <- "datasets"
topic <- "esoph"
pkgRdDB <- tools:::fetchRdDB(file.path(find.package(pkg), 'help', pkg))
tools::Rd2txt(pkgRdDB[[topic]], package = pkg, 
              options = list(underline_titles = FALSE))
```

    ## esoph                 package:datasets                 R Documentation
    ## 
    ## Smoking, Alcohol and (O)esophageal Cancer
    ## 
    ## Description:
    ## 
    ##      Data from a case-control study of (o)esophageal cancer in
    ##      Ille-et-Vilaine, France.
    ## 
    ## Usage:
    ## 
    ##      esoph
    ##      
    ## Format:
    ## 
    ##      A data frame with records for 88 age/alcohol/tobacco combinations.
    ## 
    ##        [,1]  'agegp'      Age group            1  25-34 years  
    ##                                                2  35-44        
    ##                                                3  45-54        
    ##                                                4  55-64        
    ##                                                5  65-74        
    ##                                                6  75+          
    ##        [,2]  'alcgp'      Alcohol consumption  1   0-39 gm/day 
    ##                                                2  40-79        
    ##                                                3  80-119       
    ##                                                4  120+         
    ##        [,3]  'tobgp'      Tobacco consumption  1   0- 9 gm/day 
    ##                                                2  10-19        
    ##                                                3  20-29        
    ##                                                4  30+          
    ##        [,4]  'ncases'     Number of cases                      
    ##        [,5]  'ncontrols'  Number of controls                   
    ##       
    ## Author(s):
    ## 
    ##      Thomas Lumley
    ## 
    ## Source:
    ## 
    ##      Breslow, N. E. and Day, N. E. (1980) _Statistical Methods in
    ##      Cancer Research. Volume 1: The Analysis of Case-Control Studies._
    ##      IARC Lyon / Oxford University Press.
    ## 
    ## Examples:
    ## 
    ##      require(stats)
    ##      require(graphics) # for mosaicplot
    ##      summary(esoph)
    ##      ## effects of alcohol, tobacco and interaction, age-adjusted
    ##      model1 <- glm(cbind(ncases, ncontrols) ~ agegp + tobgp * alcgp,
    ##                    data = esoph, family = binomial())
    ##      anova(model1)
    ##      ## Try a linear effect of alcohol and tobacco
    ##      model2 <- glm(cbind(ncases, ncontrols) ~ agegp + unclass(tobgp)
    ##                                               + unclass(alcgp),
    ##                    data = esoph, family = binomial())
    ##      summary(model2)
    ##      ## Re-arrange data for a mosaic plot
    ##      ttt <- table(esoph$agegp, esoph$alcgp, esoph$tobgp)
    ##      o <- with(esoph, order(tobgp, alcgp, agegp))
    ##      ttt[ttt == 1] <- esoph$ncases[o]
    ##      tt1 <- table(esoph$agegp, esoph$alcgp, esoph$tobgp)
    ##      tt1[tt1 == 1] <- esoph$ncontrols[o]
    ##      tt <- array(c(ttt, tt1), c(dim(ttt),2),
    ##                  c(dimnames(ttt), list(c("Cancer", "control"))))
    ##      mosaicplot(tt, main = "esoph data set", color = TRUE)
    ## 

Our goal now is to group this data by `agegp` and then

- for each `alcgp`
  - compute the average share of cases (relative to all observations)
    across `tobgp`
  - create logical variables indicating if there are any cases at all
- create columns indicating which combinations of `alcgp` and `tobgp`
  are in the data for that `agegp`
  - once as dummy variables, limited to the values `"20-29"` and `"30+"`
    of `tobgp`
  - and once as a single column containing all combinations with at
    least 20 observations as character vectors

With `lfdcast` all of this can be done in a single function call to
`dcast()`:

``` r
Y_lf <- lfdcast::dcast(X, by = "agegp",
                       agg(to = "alcgp", 
                           avg_share_cases = gmean(ncases / (ncases + ncontrols)),
                           any_cases = gany(ncases > 0),
                           names.fun.args = list(prefix.with.colname = TRUE)),
                       agg(to = c("alcgp", "tobgp"),
                           any_obs = as.integer(glength_gt0(ncases)),
                           to.keep = data.frame(tobgp = c("20-29", "30+"))), 
                       agg(to = NULL, 
                           big_gps = glist(paste0(alcgp, "_", tobgp)),
                           subsetq = ncases + ncontrols >= 20),
                       assert.valid.names = FALSE)
withr::with_options(
  list(digits = 3),
  print(Y_lf)
)
```

    ## Key: <agegp>
    ##    agegp avg_share_cases_alcgp_0-39g/day avg_share_cases_alcgp_40-79
    ##    <ord>                           <num>                       <num>
    ## 1: 25-34                         0.00000                      0.0000
    ## 2: 35-44                         0.01786                      0.0505
    ## 3: 45-54                         0.00543                      0.3490
    ## 4: 55-64                         0.27346                      0.3115
    ## 5: 65-74                         0.16890                      0.4519
    ## 6:   75+                         0.24074                      0.4333
    ##    avg_share_cases_alcgp_80-119 avg_share_cases_alcgp_120+
    ##                           <num>                      <num>
    ## 1:                        0.000                      0.250
    ## 2:                        0.000                      0.389
    ## 3:                        0.329                      0.854
    ## 4:                        0.633                      0.714
    ## 5:                        0.615                      0.812
    ## 6:                        1.000                      1.000
    ##    any_cases_alcgp_0-39g/day any_cases_alcgp_40-79 any_cases_alcgp_80-119
    ##                       <lgcl>                <lgcl>                 <lgcl>
    ## 1:                     FALSE                 FALSE                  FALSE
    ## 2:                      TRUE                  TRUE                  FALSE
    ## 3:                      TRUE                  TRUE                   TRUE
    ## 4:                      TRUE                  TRUE                   TRUE
    ## 5:                      TRUE                  TRUE                   TRUE
    ## 6:                      TRUE                  TRUE                   TRUE
    ##    any_cases_alcgp_120+ any_obs_0-39g/day_20-29 any_obs_0-39g/day_30+
    ##                  <lgcl>                   <int>                 <int>
    ## 1:                 TRUE                       1                     1
    ## 2:                 TRUE                       1                     1
    ## 3:                 TRUE                       1                     1
    ## 4:                 TRUE                       1                     1
    ## 5:                 TRUE                       1                     1
    ## 6:                 TRUE                       0                     1
    ##    any_obs_40-79_20-29 any_obs_40-79_30+ any_obs_80-119_20-29
    ##                  <int>             <int>                <int>
    ## 1:                   1                 1                    0
    ## 2:                   1                 1                    1
    ## 3:                   1                 1                    1
    ## 4:                   1                 1                    1
    ## 5:                   1                 0                    1
    ## 6:                   1                 1                    0
    ##    any_obs_80-119_30+ any_obs_120+_20-29 any_obs_120+_30+
    ##                 <int>              <int>            <int>
    ## 1:                  1                  1                1
    ## 2:                  1                  1                0
    ## 3:                  1                  1                1
    ## 4:                  1                  1                1
    ## 5:                  1                  1                1
    ## 6:                  0                  0                0
    ##                                                          big_gps
    ##                                                           <list>
    ## 1:                             0-39g/day_0-9g/day,40-79_0-9g/day
    ## 2:                 0-39g/day_0-9g/day,40-79_0-9g/day,40-79_10-19
    ## 3:                 0-39g/day_0-9g/day,40-79_0-9g/day,40-79_10-19
    ## 4: 0-39g/day_0-9g/day,0-39g/day_10-19,40-79_0-9g/day,40-79_10-19
    ## 5:                             0-39g/day_0-9g/day,40-79_0-9g/day
    ## 6:

To achieve the same with plain `data.table`, we need multiple calls to
`dcast()`, joins and auxiliary columns:

``` r
X[, share_cases := ncases / (ncases + ncontrols)]
Y_dt <- data.table::dcast(X, agegp ~ alcgp, 
                          fun.aggregate = list(avg = mean, 
                                               any = function(x) any(x > 0)), 
                          value.var = list("share_cases", "ncases"))

Y_dt2 <- data.table::dcast(X, agegp ~ alcgp + tobgp, 
                           fun.aggregate = function(x) as.integer(length(x) > 0),
                           value.var = "ncases", 
                           subset = .(tobgp %in% c("20-29", "30+")))
Y_dt <- merge(Y_dt, Y_dt2, all = TRUE, by = "agegp")

X[, gps := paste0(alcgp, "_", tobgp)]
Y_dt2 <- data.table::dcast(X, agegp ~ ., fun.aggregate = list, value.var = "gps", 
                           subset = .(ncases + ncontrols >= 20))
Y_dt <- merge(Y_dt, Y_dt2, all = TRUE, by = "agegp")
list_col <- lapply(Y_dt[, .], function(v) if (is.null(v)) character() else v)
Y_dt[, . := list_col]

withr::with_options(
  list(digits = 3),
  print(Y_dt)
)
```

    ## Key: <agegp>
    ##    agegp share_cases_avg_0-39g/day share_cases_avg_40-79 share_cases_avg_80-119
    ##    <ord>                     <num>                 <num>                  <num>
    ## 1: 25-34                   0.00000                0.0000                  0.000
    ## 2: 35-44                   0.01786                0.0505                  0.000
    ## 3: 45-54                   0.00543                0.3490                  0.329
    ## 4: 55-64                   0.27346                0.3115                  0.633
    ## 5: 65-74                   0.16890                0.4519                  0.615
    ## 6:   75+                   0.24074                0.4333                  1.000
    ##    share_cases_avg_120+ ncases_any_0-39g/day ncases_any_40-79 ncases_any_80-119
    ##                   <num>               <lgcl>           <lgcl>            <lgcl>
    ## 1:                0.250                FALSE            FALSE             FALSE
    ## 2:                0.389                 TRUE             TRUE             FALSE
    ## 3:                0.854                 TRUE             TRUE              TRUE
    ## 4:                0.714                 TRUE             TRUE              TRUE
    ## 5:                0.812                 TRUE             TRUE              TRUE
    ## 6:                1.000                 TRUE             TRUE              TRUE
    ##    ncases_any_120+ 0-39g/day_20-29 0-39g/day_30+ 40-79_20-29 40-79_30+
    ##             <lgcl>           <int>         <int>       <int>     <int>
    ## 1:            TRUE               1             1           1         1
    ## 2:            TRUE               1             1           1         1
    ## 3:            TRUE               1             1           1         1
    ## 4:            TRUE               1             1           1         1
    ## 5:            TRUE               1             1           1         0
    ## 6:            TRUE               0             1           1         1
    ##    80-119_20-29 80-119_30+ 120+_20-29 120+_30+
    ##           <int>      <int>      <int>    <int>
    ## 1:            0          1          1        1
    ## 2:            1          1          1        0
    ## 3:            1          1          1        1
    ## 4:            1          1          1        1
    ## 5:            1          1          1        1
    ## 6:            0          0          0        0
    ##                                                                .
    ##                                                           <list>
    ## 1:                             0-39g/day_0-9g/day,40-79_0-9g/day
    ## 2:                 0-39g/day_0-9g/day,40-79_0-9g/day,40-79_10-19
    ## 3:                 0-39g/day_0-9g/day,40-79_0-9g/day,40-79_10-19
    ## 4: 0-39g/day_0-9g/day,0-39g/day_10-19,40-79_0-9g/day,40-79_10-19
    ## 5:                             0-39g/day_0-9g/day,40-79_0-9g/day
    ## 6:

Both approaches lead to equal results, except for the column names:

``` r
stopifnot(all.equal(Y_lf, Y_dt, check.attributes = FALSE))
```
