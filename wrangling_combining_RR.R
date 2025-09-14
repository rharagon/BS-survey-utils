# =============================================
# Cargar librerías necesarias
# =============================================
library(tidyverse)

# =============================================
# SECCIÓN 1: Combinar archivos CSV de títulos
# =============================================
# Cargar los archivos CSV de títulos
multi_titles <- read_csv("C:/propios/Doc_sin_respaldo_nube/down_sb3/multi/multi_sb3_titles.csv")
nomulti_titles <- read_csv("C:/propios/Doc_sin_respaldo_nube/down_sb3/no_multi/nomulti_sb3_titles.csv")

# Asegurar consistencia en los nombres de columnas
multi_titles <- multi_titles %>%
  select("Project ID", Title) %>%
  rename(filename = "Project ID")

nomulti_titles <- nomulti_titles %>%
  select(filename, project_title) %>%
  rename(Title = project_title)

# Combinar los datasets de títulos
combined_titles <- bind_rows(multi_titles, nomulti_titles) %>%
  arrange(filename)

# Guardar el dataset combinado de títulos
write_csv(combined_titles, "C:/propios/Doc_sin_respaldo_nube/down_sb3/sb3_titles.csv")

# =============================================
# SECCIÓN 2: Consolidar resultados de Litterbox y DrScratch
# =============================================
# Cargar resultados de Litterbox y DrScratch
litter_results <- read_csv("./litter_results_all.csv")
drscratch_results <- read_csv("./01_CT_last.csv")

# Procesar datos de DrScratch
drscratch_results <- drscratch_results %>%
  mutate(project = str_remove(project, "\\.sb3$")) %>%
  mutate(project = as.double(project)) %>%
  distinct(project, .keep_all = TRUE)

# Procesar datos de Litterbox
litter_results <- litter_results %>%
  distinct(project, .keep_all = TRUE)

# Unir datasets por proyecto
dataset_unido <- litter_results %>%
  inner_join(drscratch_results, by = "project")

# =============================================
# SECCIÓN 3: Subsanar información de total_blocks - NO NECESARIO CARGAR Y UNIR CON ULTIMA VERSION DE Dr.ScratchConsole, Filtrado SI
# =============================================
total_block <- read_csv("./total_block.csv") %>%
  rename(project = project_id) %>%
  distinct(project, .keep_all = TRUE)

# Unir con información de total_blocks
dataset_unido <- dataset_unido %>%
  left_join(total_block %>% select(project, total_blocks), by = "project")
#
# Filtrar proyectos con total_blocks == 0
#
dataset_unido <- dataset_unido %>%
  filter(total_blocks != 0) |> distinct(project, .keep_all = TRUE)

df_control <- read_csv("C:/propios/Doc_sin_respaldo_nube/down_sb3/nearest_add.csv")

dataset_unido <- anti_join(dataset_unido,df_control, by="project")

# Guardar dataset unido
write_csv(dataset_unido, "C:/propios/Doc_sin_respaldo_nube/down_sb3/X_TT_add.csv")

# =============================================
# SECCIÓN 4: Combinar con dataset existente (X_TT)
# =============================================
# Cargar dataset existente
X_TT <- read_csv("./X_TT.csv")

# Renombrar columnas para consistencia
X_TT <- X_TT %>%
  rename(
    project = filename,
    duplicateScripts = duplicateScript
  )

dataset_unido <- dataset_unido %>%
  rename(sprite_count = babia_num_sprites) %>%
  select(-mastery_total_points, -mastery_total_max, -mastery_competence) %>%
  mutate(
    Author = NA,
    "Creation date" = NA,
    "Modified date" = NA,
    "Remix parent id" = NA,
    "Remix root id" = NA,
    `Project title` = NA
  ) %>%
  distinct()

X_TT <- X_TT %>%
  select(-backdrop_count) |> mutate(`Project title` = NA)

# columnas ausentes:
(missing_cols <- setdiff(colnames(X_TT), colnames(dataset_unido)))
(missing_cols <- setdiff(colnames(dataset_unido), colnames(X_TT)))
rm(missing_cols)

# Unir datasets
X_TT_actualizado <- bind_rows(X_TT, dataset_unido)

# =============================================
# SECCIÓN 5: Añadir metadatos ausentes
# =============================================
# Cargar metadatos adicionales
dataset_unido_meta <- read_csv("./01_meta_last.csv")
X_titles <- read_csv("./X_TT_titles.csv")
X_titles$project_title[is.na(X_titles$project_title)] <- ""

# Procesar títulos -- NO NECESARIO CON ULTIMA VERSION DE Dr.ScratchConsole, Filtrado SI
dataset_unido_titles <- read_csv("C:/propios/Doc_sin_respaldo_nube/down_sb3/sb3_titles.csv") %>%
  distinct() %>%
  rename(
    project = filename,
    `Project title` = Title
  )

# Actualizar dataset_unido con títulos -- NO NECESARIO CON ULTIMA VERSION DE Dr.ScratchConsole, Filtrado SI
dataset_unido <- dataset_unido %>%
  rows_update(
    dataset_unido_titles,
    by = "project", unmatched = "ignore"
  )

# Actualizar con metadatos adicionales
dataset_unido_meta <- dataset_unido_meta %>%
  rename(project = project_id)

dataset_unido <- dataset_unido |>mutate(
  `Project title` = as.character(`Project title`),
  Author = as.character(Author),
  `Creation date` = as.POSIXct(`Creation date`),
  `Modified date` = as.POSIXct(`Modified date`),
  `Remix parent id` = as.double(`Remix parent id`),
  `Remix root id` = as.double(`Remix root id`)
)

dataset_unido <- dataset_unido %>%
  rows_update(
    dataset_unido_meta %>%
      select(project, `Project title`, Author, `Creation date`,
             `Modified date`, `Remix parent id`, `Remix root id`),
    by = "project", unmatched = "ignore"
  )

# Actualizar X_TT con títulos legacy
X_TT <- X_TT %>%
  left_join(
    X_titles %>%
      rename(`Project title` = project_title),
    by = c("project" = "filename")
  )

X_TT<-X_TT|>select(-`Project title.x`)|>rename(`Project title`=`Project title.y`)

# Filtrar proyectos con ERROR o sin título
X_TT <- X_TT %>%
  filter(!str_detect(`Project title`, "ERROR"), !is.na(`Project title`), `Project title` != "")

dataset_unido <- dataset_unido %>%
  filter(!str_detect(`Project title`, "ERROR"), !is.na(`Project title`), `Project title` != "")

# =============================================
# SECCIÓN 6: Eliminar duplicados y guardar consolidado
# =============================================
# Eliminar duplicados
X_TT <- X_TT %>%
  distinct()

dataset_unido <- dataset_unido %>%
  distinct()

# Guardar datasets consolidados
write_csv(X_TT, "./01_X_TT.csv")
write_csv(dataset_unido, "./01_X_TT_adicionales.csv")

# install.packages("arrow")
arrow::write_parquet(muestra_aleatoria,"C:/propios/Doc_sin_respaldo_nube/down_sb3/X_TT_RR.parquet")

# =============================================
# SECCIÓN 7: Emplear Nearest Neighbor Matching para completar con 282,749 muestras adicionales para acercarnos a los 2Mill.
# =============================================
#set.seed(2000017)
#muestra_aleatoria <- X_TT_actualizado %>% sample_n(2000017)

rm(list = ls())
A <- read_csv("./01_X_TT.csv")
B <- read_csv("./01_X_TT_adicionales.csv")


library(MatchIt)
library(cobalt)

# ----- Parámetros principales -----
n_target <- 2000017         # tamaño final deseado
# A y B: data.frames ya en memoria


stopifnot(exists("A"), exists("B"))
nA <- nrow(A); nB <- nrow(B)
n_add <- n_target - nA
stopifnot(n_add > 0, nB >= n_add) # tenemos suficientes controles en B

# 1) Especifica las COVARIABLES para igualar la distribución
#    Incluye numéricas y categóricas que quieras preservar.
#    EJEMPLO con tus columnas:
X_vars <- c(
  "Abstraction","Parallelization","Logic","Synchronization",
  "FlowControl","UserInteractivity","DataRepresentation",
  "MathOperators","MotionOperators"
)

# Si tienes categóricas clave con solape adecuado, puedes usar exact matching:
exact_vars <- NULL     # p.ej., ~ region + cohorte_mes   (o deja NULL)
caliper    <- NULL     # p.ej., 0.2 para caliper en logit(PS); NULL = sin caliper
set.seed(7)

# ------------------ Utilidades ------------------
prep_for_model <- function(df, vars) {
  df2 <- df[, vars, drop = FALSE]
  ch  <- names(df2)[vapply(df2, is.character, logical(1))]
  if (length(ch)) df2[ch] <- lapply(df2[ch], factor)
  df2
}

# ------------------ IDs y prep ------------------
nA <- nrow(A); nB <- nrow(B)
n_add <- n_target - nA
if (n_add <= 0) stop("n_target debe ser > nrow(A)")
if (nB < n_add) stop("B no tiene suficientes filas para aportar 282,749 controles")

if (!".__id_A" %in% names(A)) A$.__id_A <- seq_len(nA)
if (!".__id_B" %in% names(B)) B$.__id_B <- seq_len(nB)

A_X <- prep_for_model(A, X_vars)
B_X <- prep_for_model(B, X_vars)

# Trabajamos con complete cases SOLO para el matching (no tocamos A/B originales)
cc_A_idx <- which(stats::complete.cases(A_X))
cc_B_idx <- which(stats::complete.cases(B_X))
if (length(cc_B_idx) < n_add) stop("Tras eliminar NAs en covariables, B no tiene capacidad suficiente")

# Submuestra de tratados A* EXACTAMENTE de tamaño n_add (desde complete cases)
A_star_idx <- sample(cc_A_idx, size = n_add, replace = FALSE)

A_star     <- A[A_star_idx, , drop = FALSE]
A_X_star   <- A_X[A_star_idx, , drop = FALSE]
# (A* ya es complete-case por construcción)

B_m        <- B[cc_B_idx, , drop = FALSE]
B_X_m      <- B_X[cc_B_idx, , drop = FALSE]

# ------------------ Data para MatchIt ------------------
A_model <- A_X_star %>%
  mutate(.treat = 1L, .src = "A_star", id_Astar = A_star$.__id_A)

B_model <- B_X_m %>%
  mutate(.treat = 0L, .src = "B", id_B = B_m$.__id_B)

dat_model <- bind_rows(A_model, B_model)

form <- as.formula(paste(".treat ~", paste(X_vars, collapse = " + ")))

# ------------------ Matching NNM 1:1 ------------------
m.args <- list(
  formula = form,
  data    = dat_model,
  method  = "nearest",
  distance= "glm",
  replace = FALSE,
  ratio   = 1,
  estimand= "ATT"
)
if (!is.null(caliper)) m.args$caliper <- caliper
if (!is.null(exact_vars)) m.args$exact <- exact_vars

m.out <- do.call(MatchIt::matchit, m.args)

# ------------------ Recuperar controles por ID ------------------
matched_dat <- match.data(m.out)

B_sel_ids <- matched_dat %>%
  filter(.treat == 0L, weights > 0) %>%
  pull(id_B) %>%
  unique()

# Verificaciones de tamaño
n_treated_matched <- sum(matched_dat$.treat == 1L & matched_dat$weights > 0)
if (length(B_sel_ids) < n_treated_matched) {
  warning("Controles < tratados emparejados; revisa caliper/overlap/exact_vars/vars.")
}
if (length(B_sel_ids) < n_add) {
  stop(sprintf("Sólo se obtuvieron %d controles; se necesitan %d. Ajusta caliper/exact_vars o reduce X_vars.",
               length(B_sel_ids), n_add))
}
# Recorta si por algún detalle excede n_add
if (length(B_sel_ids) > n_add) {
  B_sel_ids <- B_sel_ids[seq_len(n_add)]
}

# Selección SIN duplicados ni joins por covariables
# (indexamos B por su ID original en el mismo orden)
ord <- match(B_sel_ids, B$.__id_B)
B_selected <- B[ord, , drop = FALSE]

# ------------------ Dataset final ------------------
A_completed <- bind_rows(A, B_selected)
stopifnot(nrow(A_completed) == n_target,
          nrow(B_selected)  == n_add)

message("✅ OK: añadidas ", nrow(B_selected), " filas de B. Tamaño final: ", nrow(A_completed))

B_selected |> mutate(project = str_remove(project, "\\.sb3$")) %>% mutate(project = as.double(project)) %>% distinct(project, .keep_all = TRUE) |> select(project) |> write_csv("nearest_add.csv")

# 10) (Opcional) Validaciones rápidas de distribución A vs B_selected
check_summary <- function(df1, df2, vars) {
  out <- lapply(vars, function(v){
    if (is.numeric(df1[[v]]) && is.numeric(df2[[v]])) {
      tibble::tibble(
        var = v,
        mean_A = mean(df1[[v]], na.rm = TRUE),
        mean_Bsel = mean(df2[[v]], na.rm = TRUE),
        q10_A = quantile(df1[[v]], 0.1, na.rm = TRUE),
        q10_Bsel = quantile(df2[[v]], 0.1, na.rm = TRUE),
        q50_A = quantile(df1[[v]], 0.5, na.rm = TRUE),
        q50_Bsel = quantile(df2[[v]], 0.5, na.rm = TRUE),
        q90_A = quantile(df1[[v]], 0.9, na.rm = TRUE),
        q90_Bsel = quantile(df2[[v]], 0.9, na.rm = TRUE)
      )
    } else if (is.factor(df1[[v]]) || is.character(df1[[v]])) {
      pA <- prop.table(table(df1[[v]]))
      pB <- prop.table(table(df2[[v]]))
      lev <- union(names(pA), names(pB))
      d <- data.frame(level = lev,
                      pA = as.numeric(pA[lev]),
                      pBsel = as.numeric(pB[lev]))
      d$var <- v
      tibble::as_tibble(d[, c("var","level","pA","pBsel")])
    } else {
      tibble::tibble(var = v, note = "tipo no evaluado")
    }
  })
  dplyr::bind_rows(out)
}

A_completed <- A_completed %>%
  distinct()

check_vars <- X_vars
dist_check <- check_summary(A, B, check_vars)
dist_check <- check_summary(A, B_selected, check_vars)
print(head(dist_check, 20))

dist_check|>write_csv("check_AvsB_select.csv")



# Guardar muestra combinada con técnica nearest
write_csv(A_completed, "./01_X_TT_RR.csv")
write_csv(B_selected, "./01_X_TT_adicionales_selected.csv")
arrow::write_parquet(A_completed,"./X_TT_RR.parquet")
