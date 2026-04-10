#!/usr/bin/env bash

# Shared dataset-specific host preprocessing parameters for DDR flows.
#
# Usage:
#   resolve_ddr_dataset_config <dataset_filename>
#
# Output:
#   "<dataset_alias>\t<BIG_EDGE_PER_MS>\t<LITTLE_EDGE_PER_MS>"

resolve_ddr_dataset_config() {
  case "$1" in
    graph500-scale23-ef16_adj.mtx) printf '%s\t%s\t%s\n' "graph500" "400000" "1000000" ;;
    rmat-19-32.txt) printf '%s\t%s\t%s\n' "r19" "250000" "950000" ;;
    rmat-21-32.txt) printf '%s\t%s\t%s\n' "r21" "290000" "1000000" ;;
    rmat-24-16.txt) printf '%s\t%s\t%s\n' "r24" "270000" "1000000" ;;
    amazon-2008.mtx) printf '%s\t%s\t%s\n' "am" "160000" "460000" ;;
    ca-hollywood-2009.mtx) printf '%s\t%s\t%s\n' "hollywood" "300000" "1000000" ;;
    dbpedia-link.mtx) printf '%s\t%s\t%s\n' "dbpedia" "190000" "900000" ;;
    soc-flickr-und.mtx) printf '%s\t%s\t%s\n' "flickr" "120000" "800000" ;;
    soc-LiveJournal1.txt) printf '%s\t%s\t%s\n' "LiveJournal1" "170000" "700000" ;;
    soc-orkut-dir.mtx) printf '%s\t%s\t%s\n' "orkut" "280000" "850000" ;;
    web-baidu-baike.mtx) printf '%s\t%s\t%s\n' "baidu" "160000" "800000" ;;
    web-Google.mtx) printf '%s\t%s\t%s\n' "Google" "150000" "580000" ;;
    web-hudong.mtx) printf '%s\t%s\t%s\n' "hudong" "180000" "850000" ;;
    wiki-topcats.txt) printf '%s\t%s\t%s\n' "topcats" "170000" "830000" ;;
    *) return 1 ;;
  esac
}
