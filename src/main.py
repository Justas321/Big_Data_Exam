from preprocess_ais import main as preprocess_main
from detect_collision import main as detect_main
from rank_collision_candidates import main as rank_main
from visualize_collision import main as visualize_main
from export_readable_result import main as export_result_main

# Simple pipeline entrypoint. Uncomment stages as needed.
# Typical workflow: preprocess -> detect candidates -> rank -> visualize.
if __name__ == "__main__":
    preprocess_main()
    detect_main()
    rank_main()
    visualize_main()
    export_result_main()