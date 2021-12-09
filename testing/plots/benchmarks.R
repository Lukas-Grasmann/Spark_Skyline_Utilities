library('dplyr')
library('stringr')
library('ggplot2')
library('scales')
library('xtable')
library('tikzDevice')

# pre-define colorblind friendly palette
cbbPalette <- c('#000000', '#E69F00', '#56B4E9', '#009E73', '#F0E442',
                '#0072B2', '#D55E00', '#CC79A7', '#ff00ff', '#666666')

names(cbbPalette) <- as.factor(c('reference', 'dist', 'dist_inc', 'bnl'))

# load and preprocess data
report.raw <- read.csv('data/output.csv')
# convert all columns to factors where sensible
report.factor <- report.raw %>%
  # convert all character columns to factors
  mutate_if(sapply(report.raw, is.character), as.factor) %>%
  # convert the dimension column to factors (limited number of options)
  mutate(dimensions = as.factor(dimensions)) %>%
  # convert the nodes column to factors (limited number of options)
  mutate(nodes = as.factor(nodes))
  # convert the size column to factors (limited number of options)
  # mutate(size = as.factor(size))

# number of dimensions vs execution time
# split by query, input size, nodes, dataset
report.dimensions_vs_time <- report.factor %>%
  group_by(query, size, nodes, dataset) %>%
  group_split()

# input size vs execution time
# split by query, dimensions, nodes, dataset
report.size_vs_time <- report.factor %>%
  # group_by(query, dimensions, nodes, dataset) %>%
  group_by(query, dimensions, nodes) %>%
  filter(!str_detect(dataset, 'incomplete')) %>%
  group_split()

# number of available nodes vs execution time
# split by query, dimensions size, dataset
report.nodes_vs_time <- report.factor %>%
  group_by(query, dimensions, size, dataset) %>%
  group_split()

for (report.iter in report.dimensions_vs_time) {
  query <- report.iter$query[[1]]
  size <- report.iter$size[[1]]
  nodes <- report.iter$nodes[[1]]
  dataset <- report.iter$dataset[[1]]

  plot <- ggplot(
    data = report.iter,
    aes(x=dimensions, y=time, group=algorithm)) +
    geom_line(size=1.1, aes(color=algorithm)) +
    geom_point(size=1.3, aes(color=algorithm)) +
    ggtitle('Benchmark Dimension vs. Execution Time',
            subtitle = paste(
              query,
              paste('size', size, sep=': '),
              paste('nodes', nodes, sep=': '),
              paste('dataset', dataset, sep=': '),
              sep = ', ')) +
    labs(x='No. of dimensions', y='time [s]')

  plot.log <- plot +
    # scale_y_continuous(trans = log10_trans()) +
    theme(legend.position = 'right',
          legend.direction = 'vertical') +
    scale_color_manual(values=cbbPalette, name='Algorithm', drop = TRUE, limits = force)

  ggsave(paste(
    'dimension_vs_time', '-',
    gsub(' ', '_', dataset), '-',
    gsub(' ', '_', query), '-',
    gsub(' ', '_', size), 't', '-',
    gsub(' ', '_', nodes), 'n',
    '.png' , sep = ''), plot.log, width=10, height=4)
}

for (report.iter in report.size_vs_time) {
  query <- report.iter$query[[1]]
  dimensions <- report.iter$dimensions[[1]]
  nodes <- report.iter$nodes[[1]]
  # dataset <- report.iter$dataset[[1]]

  plot <- ggplot(
    data = report.iter,
    aes(x=size, y=time, group=algorithm))+
    geom_line(size=1.1, aes(color=algorithm)) +
    geom_point(size=1.3, aes(color=algorithm)) +
    ggtitle('Benchmark Size vs. Execution Time',
            subtitle = paste(
              query,
              paste('dimensions', dimensions, sep=': '),
              paste('nodes', nodes, sep=': '),
              # paste('dataset', dataset, sep=': '),
              sep = ', ')) +
    labs(x='dataset size', y='time [s]')

  plot.log <- plot +
    # scale_y_continuous(trans = log10_trans()) +
    theme(legend.position = 'right',
          axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
          legend.direction = 'vertical') +
    scale_color_manual(values=cbbPalette, name='Algorithm', drop = TRUE, limits = force)

  ggsave(paste(
    'size_vs_time', '-',
    gsub(' ', '_', dataset), '-',
    gsub(' ', '_', query), '-',
    gsub(' ', '_', dimensions), 'd', '-',
    gsub(' ', '_', nodes), 'n',
    '.png' , sep = ''), plot.log, width=10, height=4)
}

options(scipen=99)

for (report.iter in report.nodes_vs_time) {
  query <- report.iter$query[[1]]
  dimensions <- report.iter$dimensions[[1]]
  size <- report.iter$size[[1]]
  dataset <- report.iter$dataset[[1]]

  plot <- ggplot(
    data = report.iter,
    aes(x=nodes, y=time, group=algorithm)) +
    geom_line(size=1.1, aes(color=algorithm)) +
    geom_point(size=1.3, aes(color=algorithm)) +
    ggtitle('Benchmark Nodes vs. Execution Time',
            subtitle = paste(
              query,
              paste('dimensions', dimensions, sep=': '),
              paste('size', size, sep=': '),
              paste('dataset', dataset, sep=': '),
              sep = ', ')) +
    labs(x='number of nodes', y='time [s]')

  plot.log <- plot +
    # scale_y_continuous(trans = log10_trans()) +
    theme(legend.position = 'right',
          legend.direction = 'vertical') +
    scale_color_manual(values=cbbPalette, name='Algorithm', drop = TRUE, limits = force)

  ggsave(paste(
    'nodes_vs_time', '-',
    gsub(' ', '_', dataset), '-',
    gsub(' ', '_', query), '-',
    gsub(' ', '_', size), 't', '-',
    gsub(' ', '_', dimensions), 'd',
    '.png', sep = ''), plot.log, width=10, height=4)
}
