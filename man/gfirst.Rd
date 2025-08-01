% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/zzz.R
\name{gfirst}
\alias{gfirst}
\alias{glast}
\alias{gsample}
\title{Aggregate by Selecting one Element}
\usage{
gfirst(x, na.rm = FALSE, fill = NA)

glast(x, na.rm = FALSE, fill = NA)

gsample(x, na.rm = FALSE, fill = NA)
}
\arguments{
\item{x}{an expression to be evaluated in the context of the data frame
\code{\link[lfdcast:dcast]{X}} to cast and resulting in an atomic vector of
length \code{nrow(\link[lfdcast:dcast]{X})}. In the simplest case this is
just an unquoted column name of the data frame
\code{\link[lfdcast:dcast]{X}}.}

\item{na.rm}{should \code{NA}s be removed from the vector resulting from the
evaluation of \code{x} before applying the aggregation function to it?}

\item{fill}{value with which to fill empty cells in the result.}
}
\description{
Aggregate by Selecting one Element
}
