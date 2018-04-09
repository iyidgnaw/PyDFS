# This file contains all default configuration for all components to read from
# Note: this is the single source of truth about the whole system.

# Master server configuration
block_size = 10
replication_factor = 3

# Default values for starting all services
default_minion_ports = [8888, 8889, 8890, 8891]
default_proxy_port = 2130
default_master_port = 2131

other_masters = [2132, 2133]
