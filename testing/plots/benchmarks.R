library('dplyr')
library('stringr')
library('ggplot2')
library('scales')
library('xtable')
library('tikzDevice')

# pre-define colorblind friendly palette
cbbPalette <- c('#000000', '#E69F00', '#56B4E9', '#009E73', '#F0E442',
                '#0072B2', '#D55E00', '#CC79A7', '#ff00ff', '#666666')

names(cbbPalette) <- as.factor(c('Reference', 'Distributed Complete', 'Distributed Incomplete', 'Block-Nested-Loop'))

nodes_text = 'Nodes'

# load and preprocess data
report.raw <- read.csv('data/output.csv') %>%
  mutate(algorithm=replace(algorithm, algorithm=='reference', 'Reference')) %>%
  mutate(algorithm=replace(algorithm, algorithm=='bnl', 'Block-Nested-Loop')) %>%
  mutate(algorithm=replace(algorithm, algorithm=='dist_inc', 'Distributed Incomplete')) %>%
  mutate(algorithm=replace(algorithm, algorithm=='dist', 'Distributed Complete'))
# convert all columns to factors where sensible
report.factor <- report.raw %>%
  # convert all character columns to factors
  mutate_if(sapply(report.raw, is.character), as.factor) %>%
  mutate(memory = memory / (1024^2))
  # convert the dimension column to factors (limited number of options)
  # mutate(dimensions = as.factor(dimensions)) %>%
  # convert the nodes column to factors (limited number of options)
  # mutate(nodes = as.factor(nodes))
  # convert the size column to factors (limited number of options)
  # mutate(size = as.factor(size))

# number of dimensions vs execution time
# split by query, input size, nodes, dataset
report.dimensions_vs_time <- report.factor %>%
  group_by(query, size, nodes, dataset) %>%
  group_split()

# number of dimensions vs memory consumption
# split by query, input size, nodes, dataset
report.dimensions_vs_memory <- report.factor %>%
  group_by(query, size, nodes, dataset) %>%
  group_split()

# input size vs execution time
# split by query, dimensions, nodes, dataset
report.size_vs_time <- report.factor %>%
  # group_by(query, dimensions, nodes, dataset) %>%
  group_by(query, dimensions, nodes) %>%
  filter(!str_detect(dataset, 'incomplete')) %>%
  group_split()

# input size vs memory consumption
# split by query, dimensions, nodes, dataset
report.size_vs_memory <- report.factor %>%
  # group_by(query, dimensions, nodes, dataset) %>%
  group_by(query, dimensions, nodes) %>%
  filter(!str_detect(dataset, 'incomplete')) %>%
  group_split()

# number of available nodes vs execution time
# split by query, dimensions size, dataset
report.nodes_vs_time <- report.factor %>%
  group_by(query, dimensions, size, dataset) %>%
  group_split()

# number of available nodes vs memory consumption
# split by query, dimensions size, dataset
report.nodes_vs_memory <- report.factor %>%
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
              paste('Input Tuples', size, sep=': '),
              paste(paste('Number of', nodes_text, sep=' '), nodes, sep=': '),
              paste('Dataset', dataset, sep=': '),
              sep = ' | ')) +
    labs(x='Number of Dimensions', y='Execution Time [s]') +
    theme_bw(base_size = 14) +
    theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))
  
  plot.log <- plot +
    scale_x_continuous(
      breaks = c(sort(unique(report.iter$dimensions))),
      labels = c(sort(unique(report.iter$dimensions)))) +
    scale_y_continuous(trans = log10_trans()) +
    expand_limits(y = 0) +
    theme(legend.position = 'right',
          legend.direction = 'vertical') +
    scale_color_manual(values=cbbPalette, name='Algorithm', drop = TRUE, limits = force)

  ggsave(paste(
    'dimension_vs_time', '-',
    gsub(' ', '_', dataset), '-',
    gsub(' ', '_', query), '-',
    gsub(' ', '_', size), 't', '-',
    gsub(' ', '_', nodes), 'n',
    '.png' , sep = ''), plot.log, width=10, height=4, dpi=300)
}

for (report.iter in report.dimensions_vs_memory) {
  query <- report.iter$query[[1]]
  size <- report.iter$size[[1]]
  nodes <- report.iter$nodes[[1]]
  dataset <- report.iter$dataset[[1]]
  
  plot <- ggplot(
    data = report.iter,
    aes(x=dimensions, y=memory, group=algorithm)) +
    geom_line(size=1.1, aes(color=algorithm)) +
    geom_point(size=1.3, aes(color=algorithm)) +
    ggtitle('Benchmark Dimension vs. Memory Consumption',
            subtitle = paste(
              paste('Input Tuples', size, sep=': '),
              paste(paste('Number of', nodes_text, sep=' '), nodes, sep=': '),
              paste('Dataset', dataset, sep=': '),
              sep = ' | ')) +
    labs(x='Number of Dimensions', y='Peak Memory Consumption [MB]') +
    theme_bw(base_size = 14) +
    theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))
  
  plot.log <- plot +
    scale_x_continuous(
      breaks = c(sort(unique(report.iter$dimensions))),
      labels = c(sort(unique(report.iter$dimensions)))) +
    scale_y_continuous(trans = log10_trans()) +
    expand_limits(y = 0) +
    theme(legend.position = 'right',
          legend.direction = 'vertical') +
    scale_color_manual(values=cbbPalette, name='Algorithm', drop = TRUE, limits = force)
  
  ggsave(paste(
    'dimension_vs_memory', '-',
    gsub(' ', '_', dataset), '-',
    gsub(' ', '_', query), '-',
    gsub(' ', '_', size), 't', '-',
    gsub(' ', '_', nodes), 'n',
    '.png' , sep = ''), plot.log, width=10, height=4, dpi=300)
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
              paste('Skyline Dimensions', dimensions, sep=': '),
              paste(paste('Number of', nodes_text, sep=' '), nodes, sep=': '),
              # paste('dataset', dataset, sep=': '),
              sep = ' | ')) +
    labs(x='Dataset Size [MB]', y='Execution Time [s]') +
    theme_bw(base_size = 14) +
    theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))

  plot.log <- plot +
    scale_x_continuous(trans = log10_trans()) +
    scale_y_continuous(trans = log10_trans()) +
    expand_limits(x = 0, y = 0) +
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
    '.png' , sep = ''), plot.log, width=10, height=4, dpi=300)
}

for (report.iter in report.size_vs_memory) {
  query <- report.iter$query[[1]]
  dimensions <- report.iter$dimensions[[1]]
  nodes <- report.iter$nodes[[1]]
  # dataset <- report.iter$dataset[[1]]
  
  plot <- ggplot(
    data = report.iter,
    aes(x=size, y=memory, group=algorithm))+
    geom_line(size=1.1, aes(color=algorithm)) +
    geom_point(size=1.3, aes(color=algorithm)) +
    ggtitle('Benchmark Size vs. Memory Consumption',
            subtitle = paste(
              paste('Skyline Dimensions', dimensions, sep=': '),
              paste(paste('Number of', nodes_text, sep=' '), nodes, sep=': '),
              # paste('dataset', dataset, sep=': '),
              sep = ' | ')) +
    labs(x='Dateset Size [MB]', y='Peak Memory Consumption [MB]') +
    theme_bw(base_size = 14) +
    theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))
  
  plot.log <- plot +
    scale_x_continuous(trans = log10_trans()) +
    scale_y_continuous(trans = log10_trans()) +
    expand_limits(x = 0, y = 0) +
    theme(legend.position = 'right',
          axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
          legend.direction = 'vertical') +
    scale_color_manual(values=cbbPalette, name='Algorithm', drop = TRUE, limits = force)
  
  ggsave(paste(
    'size_vs_memory', '-',
    gsub(' ', '_', dataset), '-',
    gsub(' ', '_', query), '-',
    gsub(' ', '_', dimensions), 'd', '-',
    gsub(' ', '_', nodes), 'n',
    '.png' , sep = ''), plot.log, width=10, height=4, dpi=300)
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
    ggtitle(paste('Number of ', nodes_text, ' vs. Execution Time', sep=''),
            subtitle = paste(
              paste('Skyline Dimensions', dimensions, sep=': '),
              paste('Input Tuples', size, sep=': '),
              paste('Dataset', dataset, sep=': '),
              sep = ' | ')) +
    labs(x=paste('Number of ', nodes_text, sep=''), y='Execution Time [s]') +
    theme_bw(base_size = 14) +
    theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))

  plot.log <- plot +
    scale_x_continuous(
      breaks = seq(min(report.iter$nodes), max(report.iter$nodes)),
      labels = seq(min(report.iter$nodes), max(report.iter$nodes))) +
    scale_y_continuous(trans = log10_trans()) +
    expand_limits(y = 0) +
    theme(legend.position = 'right',
          legend.direction = 'vertical') +
    scale_color_manual(values=cbbPalette, name='Algorithm', drop = TRUE, limits = force)

  ggsave(paste(
    'nodes_vs_time', '-',
    gsub(' ', '_', dataset), '-',
    gsub(' ', '_', query), '-',
    gsub(' ', '_', size), 't', '-',
    gsub(' ', '_', dimensions), 'd',
    '.png', sep = ''), plot.log, width=10, height=4, dpi=300)
}

for (report.iter in report.nodes_vs_memory) {
  query <- report.iter$query[[1]]
  dimensions <- report.iter$dimensions[[1]]
  size <- report.iter$size[[1]]
  dataset <- report.iter$dataset[[1]]
  
  plot <- ggplot(
    data = report.iter,
    aes(x=nodes, y=memory, group=algorithm)) +
    geom_line(size=1.1, aes(color=algorithm)) +
    geom_point(size=1.3, aes(color=algorithm)) +
    ggtitle(paste('Number of ', nodes_text, ' vs. Memory Consumption', sep=''),
            subtitle = paste(
              paste('Skyline Dimensions', dimensions, sep=': '),
              paste('Input Tuples', size, sep=': '),
              paste('Dataset', dataset, sep=': '),
              sep = ' | ')) +
    labs(x=paste('Number of ', nodes_text, sep=''), y='Peak Memory Consumption [MB]') +
    theme_bw(base_size = 14) +
    theme(plot.title = element_text(hjust = 0.5), plot.subtitle = element_text(hjust = 0.5))
  
  plot.log <- plot +
    scale_x_continuous(
      breaks = seq(min(report.iter$nodes), max(report.iter$nodes)),
      labels = seq(min(report.iter$nodes), max(report.iter$nodes))) +
    scale_y_continuous(trans = log10_trans()) +
    expand_limits(y = 0) +
    theme(legend.position = 'right',
          legend.direction = 'vertical') +
    scale_color_manual(values=cbbPalette, name='Algorithm', drop = TRUE, limits = force)
  
  ggsave(paste(
    'nodes_vs_memory', '-',
    gsub(' ', '_', dataset), '-',
    gsub(' ', '_', query), '-',
    gsub(' ', '_', size), 't', '-',
    gsub(' ', '_', dimensions), 'd',
    '.png', sep = ''), plot.log, width=10, height=4, dpi=300)
}