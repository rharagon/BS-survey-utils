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
litter_results <- read_csv("C:/propios/Doc_sin_respaldo_nube/down_sb3/litter_results_all.csv")
drscratch_results <- read_csv("C:/propios/Doc_sin_respaldo_nube/down_sb3/CT_results_nuevo.csv")

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
# SECCIÓN 3: Subsanar información de total_blocks
# =============================================
total_block <- read_csv("C:/propios/Doc_sin_respaldo_nube/down_sb3/total_block.csv") %>%
  rename(project = project_id) %>%
  distinct(project, .keep_all = TRUE)

# Unir con información de total_blocks
dataset_unido <- dataset_unido %>%
  left_join(total_block %>% select(project, total_blocks), by = "project")

# Filtrar proyectos con total_blocks == 0
dataset_unido <- dataset_unido %>%
  filter(total_blocks != 0)

# Guardar dataset unido
write_csv(dataset_unido, "C:/propios/Doc_sin_respaldo_nube/down_sb3/X_TT_add.csv")

# =============================================
# SECCIÓN 4: Combinar con dataset existente (X_TT)
# =============================================
# Cargar dataset existente
X_TT <- read_csv("C:/propios/Doc_sin_respaldo_nube/down_sb3/X_TT.csv")

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
  select(-backdrop_count)

# Unir datasets
X_TT_actualizado <- bind_rows(X_TT, dataset_unido)

# =============================================
# SECCIÓN 5: Añadir metadatos ausentes
# =============================================
# Cargar metadatos adicionales
dataset_unido_meta <- read_csv("C:/propios/Doc_sin_respaldo_nube/down_sb3/CT_results_meta_nuevo.csv")
X_titles <- read_csv("C:/propios/Doc_sin_respaldo_nube/down_sb3/X_TT_titles.csv")
X_titles$project_title[is.na(X_titles$project_title)] <- ""

# Procesar títulos
dataset_unido_titles <- read_csv("C:/propios/Doc_sin_respaldo_nube/down_sb3/sb3_titles.csv") %>%
  distinct() %>%
  rename(
    project = filename,
    `Project title` = Title
  )

# Actualizar dataset_unido con títulos
dataset_unido <- dataset_unido %>%
  rows_update(
    dataset_unido_titles,
    by = "project", unmatched = "ignore"
  )

# Actualizar con metadatos adicionales
dataset_unido_meta <- dataset_unido_meta %>%
  rename(project = project_id)

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

# Filtrar proyectos con ERROR o sin título
X_TT <- X_TT %>%
  filter(!str_detect(`Project title`, "ERROR"), !is.na(`Project title`), `Project title` != "")

dataset_unido <- dataset_unido %>%
  filter(!str_detect(`Project title`, "ERROR"), !is.na(`Project title`), `Project title` != "")

# =============================================
# SECCIÓN 6: Eliminar duplicados y guardar consolidado
# =============================================
# Eliminar duplicados
X_TT_actualizado <- X_TT_actualizado %>%
  distinct()

# Guardar dataset consolidado
write_csv(X_TT_actualizado, "C:/propios/Doc_sin_respaldo_nube/down_sb3/X_TT_completo.csv")
install.packages("arrow")
arrow::write_parquet(X_TT_actualizado,"C:/propios/Doc_sin_respaldo_nube/down_sb3/X_TT_RR.parquet")


