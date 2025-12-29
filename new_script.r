#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(readxl)
  library(janitor)
  library(dplyr)
  library(lmtest)
  library(broom)
})

# =========================
# CONFIG
# =========================
setwd("/Users/iakovvainsthein/dev/rx/exp/rx_data_gatherer/")

INPUT_FILE <- "sec_submissions_features_regression.xlsx"
SHEET_NAME <- 1
OUTPUT_DIR <- "regression_output"
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

to_num <- function(x) suppressWarnings(as.numeric(x))
safe_div <- function(a, b) { b[b == 0] <- NA_real_; a / b }

# Optional: winsorize/cap extreme ratios for stability (keeps NAs as-is)
cap <- function(x, p = 0.998) {
  if (!is.numeric(x)) return(x)
  ok <- is.finite(x)
  if (sum(ok, na.rm=TRUE) < 10) return(x)
  hi <- suppressWarnings(as.numeric(quantile(x[ok], p, na.rm=TRUE)))
  lo <- suppressWarnings(as.numeric(quantile(x[ok], 1 - p, na.rm=TRUE)))
  x <- pmin(x, hi)
  x <- pmax(x, lo)
  x
}

# =========================
# READ
# =========================
cat("Reading Excel...\n")
df0 <- read_excel(INPUT_FILE, sheet = SHEET_NAME) %>%
  as.data.frame() %>%
  clean_names()

# Expect these after clean_names()
needed <- c(
  "cik","assetsbefore","daysin","ebitbefore","emplbefore","incomebebefore",
  "intercompanypct","liabbefore","netincomebefore","filingrate",
  "late_filer_flag_180d","ceo_replaced","chapter","claimsagent","commcred",
  "emerge","prepackaged","voluntary"
)

missing <- setdiff(needed, names(df0))
if (length(missing)) {
  stop("Missing required columns: ", paste(missing, collapse = ", "))
}

# =========================
# BUILD MODEL FRAME
# =========================
df <- df0 %>%
  transmute(
    cik = cik,
    
    # outcome
    emerge = to_num(emerge),
    
    # keep raw assets for denominators + sanity
    assetsbefore = to_num(assetsbefore),
    
    # logs (size controls)
    log_assets = log1p(pmax(to_num(assetsbefore), 0)),
    log_employees = log1p(pmax(to_num(emplbefore), 0)),
    
    # ratios
    ebit_over_assets       = safe_div(to_num(ebitbefore), to_num(assetsbefore)),
    incomebe_over_assets   = safe_div(to_num(incomebebefore), to_num(assetsbefore)),
    netincome_over_assets  = safe_div(to_num(netincomebefore), to_num(assetsbefore)),
    assets_over_liab       = safe_div(to_num(assetsbefore), to_num(liabbefore)),
    
    # controls (as you listed)
    voluntary = voluntary,
    filingrate = to_num(filingrate),
    ceo_replaced = ceo_replaced,
    chapter = chapter,
    claimsagent = claimsagent,
    commcred = commcred,
    
    # full model extras
    prepackaged = prepackaged,
    
    # your incremental var
    late_filer_flag_180d = to_num(late_filer_flag_180d)
  )

# ---- enforce binary outcome 0/1
u <- sort(unique(na.omit(df$emerge)))
if (length(u) != 2) stop("emerge must be binary. unique(emerge)=", paste(u, collapse=", "))
df$emerge <- ifelse(df$emerge == max(u), 1, 0)

# ---- cap extreme ratios (optional but recommended)
df$ebit_over_assets      <- cap(df$ebit_over_assets)
df$incomebe_over_assets  <- cap(df$incomebe_over_assets)
df$netincome_over_assets <- cap(df$netincome_over_assets)
df$assets_over_liab      <- cap(df$assets_over_liab)

# ---- convert categoricals to factors
as_factor <- c("voluntary","ceo_replaced","chapter","claimsagent","commcred","prepackaged")
for (nm in intersect(as_factor, names(df))) {
  df[[nm]] <- as.factor(as.character(df[[nm]]))
}

# =========================
# NA HANDLING (IMPUTE ONCE ON SUPERSET)
# =========================
impute_superset <- function(d, ycol="emerge") {
  stopifnot(ycol %in% names(d))
  for (nm in setdiff(names(d), ycol)) {
    x <- d[[nm]]
    
    if (is.numeric(x) || is.integer(x)) {
      xx <- to_num(x)
      med <- suppressWarnings(median(xx, na.rm = TRUE))
      if (is.na(med)) med <- 0
      xx[is.na(xx) | !is.finite(xx)] <- med
      d[[nm]] <- xx
    } else {
      xc <- as.character(x)
      xc[xc %in% c("", "NA", "NaN", "nan", "NULL", "null")] <- NA
      xc[is.na(xc)] <- "MISSING"
      d[[nm]] <- factor(xc)
    }
  }
  d
}

drop_degenerate <- function(d, ycol="emerge") {
  stopifnot(ycol %in% names(d))
  keep <- sapply(setdiff(names(d), ycol), function(nm) {
    x <- d[[nm]]
    if (is.factor(x)) return(nlevels(droplevels(x)) >= 2)
    ux <- unique(x[!is.na(x)])
    length(ux) >= 2
  })
  d[, c(ycol, names(keep)[keep]), drop=FALSE]
}

# =========================
# MODEL SPECS (YOUR EXACT PLAN)
# =========================
base_vars <- c(
  "ebit_over_assets",
  "incomebe_over_assets",
  "assets_over_liab",
  "netincome_over_assets",
  "log_assets",
  "log_employees",
  "voluntary",
  "filingrate",
  "ceo_replaced",
  "chapter",
  "claimsagent",
  "commcred",
  ""
)

# Superset for consistent sample across models
all_vars <- unique(c("emerge", base_vars, "prepackaged", "late_filer_flag_180d"))
df_sup <- df[, intersect(all_vars, names(df)), drop=FALSE]

# Drop rows only if outcome missing (predictor NAs get imputed)
df_sup <- df_sup[!is.na(df_sup$emerge), , drop=FALSE]

# NA report BEFORE imputation (useful for proposal writeup)
na_report <- sapply(df_sup, function(x) sum(is.na(x) | (is.numeric(x) & !is.finite(x))))
na_report <- sort(na_report, decreasing=TRUE)
write.csv(
  data.frame(var=names(na_report), na_count=as.integer(na_report)),
  file.path(OUTPUT_DIR, "na_report_superset.csv"),
  row.names=FALSE
)

# Impute once on superset, then drop degenerate predictors once
df_sup <- impute_superset(df_sup, ycol="emerge")
df_sup <- drop_degenerate(df_sup, ycol="emerge")

cat("Rows used (after imputation on superset): ", nrow(df_sup), "\n", sep="")

# Re-intersect after degenerate drop
base_vars2 <- intersect(base_vars, names(df_sup))
has_pre <- "prepackaged" %in% names(df_sup)
has_late <- "late_filer_flag_180d" %in% names(df_sup)

make_f <- function(vars) {
  if (length(vars) == 0) stop("No predictors left after filtering.")
  as.formula(paste0("emerge ~ ", paste(vars, collapse=" + ")))
}

# =========================
# FIT MODELS
# =========================
# M1: base
f1 <- make_f(base_vars2)
m1 <- glm(f1, data=df_sup, family=binomial())

# M2: base + prepackaged
m2 <- NULL
if (has_pre) {
  f2 <- make_f(c(base_vars2, "prepackaged"))
  m2 <- glm(f2, data=df_sup, family=binomial())
}

# M3: base + late_filer_flag_180d
m3 <- NULL
if (has_late) {
  f3 <- make_f(c(base_vars2, "late_filer_flag_180d"))
  m3 <- glm(f3, data=df_sup, family=binomial())
}

# M4: (FULL model = base + prepackaged) + late_filer_flag_180d
# i.e. add late filer on top of the full (prepackaged-included) model
m4 <- NULL
if (has_pre && has_late) {
  f4 <- make_f(c(base_vars2, "prepackaged", "late_filer_flag_180d"))
  m4 <- glm(f4, data=df_sup, family=binomial())
}

# =========================
# OUTPUT
# =========================
save_tidy <- function(model, name) {
  if (is.null(model)) return(invisible(NULL))
  out <- broom::tidy(model) %>%
    mutate(
      odds_ratio = exp(estimate),
      conf_low_or = exp(estimate - 1.96 * std.error),
      conf_high_or = exp(estimate + 1.96 * std.error)
    )
  write.csv(out, file.path(OUTPUT_DIR, paste0(name, "_coeffs.csv")), row.names=FALSE)
  capture.output(summary(model), file = file.path(OUTPUT_DIR, paste0(name, "_summary.txt")))
}

save_tidy(m1, "M1_base")
save_tidy(m2, "M2_base_plus_prepackaged")
save_tidy(m3, "M3_base_plus_latefiler")
save_tidy(m4, "M4_full_plus_latefiler")  # IMPORTANT: this is full(prepackaged) + late

# LR tests (nested, same dataset)
sink(file.path(OUTPUT_DIR, "lr_tests.txt"))
cat("LR TESTS (same dataset; NAs imputed on superset)\n\n")

if (!is.null(m2)) {
  cat("M1 vs M2 (adds prepackaged):\n")
  print(lrtest(m1, m2)); cat("\n")
}
if (!is.null(m3)) {
  cat("M1 vs M3 (adds late_filer_flag_180d):\n")
  print(lrtest(m1, m3)); cat("\n")
}
if (!is.null(m4) && !is.null(m2)) {
  cat("M2 vs M4 (adds late_filer_flag_180d on top of FULL model w/ prepackaged):\n")
  print(lrtest(m2, m4)); cat("\n")
}
if (!is.null(m4) && !is.null(m3)) {
  cat("M3 vs M4 (adds prepackaged on top of late_filer):\n")
  print(lrtest(m3, m4)); cat("\n")
}
sink()

cat("Done. Outputs saved to: ", OUTPUT_DIR, "\n", sep="")


