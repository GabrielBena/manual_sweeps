import numpy as np

import secrets
import string
import os
from pathlib import Path

import joblib
import yaml
import json
from itertools import product

try:
    import cPickle as pickle
except ModuleNotFoundError:
    import pickle

from filelock import Timeout, FileLock
import time
from json.decoder import JSONDecodeError


def dict_product(varying_params, excluded_params={}):
    return [
        {
            k: p
            for k, p in zip(varying_params.keys(), params)
            if k not in excluded_params
        }
        for params in product(*varying_params.values())
    ]


def generate_id(length: int = 8) -> str:
    """Generate a random base-36 string of `length` digits."""
    # There are ~2.8T base-36 8-digit strings. If we generate 210k ids,
    # we'll have a ~1% chance of collision.
    alphabet = string.ascii_lowercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def load_params(path, use_json=False):
    if use_json:
        with open(path, "r") as f:
            # Read each line and parse the JSON string back into a dictionary
            os.fsync(f.fileno())
            return [json.loads(line) for line in f]

    else:
        try:
            with open(path, "rb") as f:
                os.fsync(f.fileno())
                return pickle.load(f), False

        except EOFError:
            # Retry Loading
            return None, True


def save_params(path, all_params, use_json=False):
    if use_json:
        with open(path, "w") as f:
            # Iterate over list of dictionaries and write each one on a new line
            for d in all_params:
                f.write(json.dumps(d) + "\n")

            f.flush()
            os.fsync(f.fileno())

    else:
        with open(path, "wb") as f:
            pickle.dump(all_params, f)

            f.flush()
            os.fsync(f.fileno())


def generate_sweep(varying_params, d_path):
    # ------ Create Sweep Id (length 8) ------

    sweep_id = generate_id()
    sweep_path = f"{d_path}/sweeps/{sweep_id}"
    path = Path(sweep_path)
    path.mkdir(exist_ok=True, parents=True)
    varying_params["sweep_id"] = [sweep_id]
    all_params = dict_product(varying_params)

    # joblib.dump(all_params, f"{sweep_path}/all_params")
    save_params(f"{sweep_path}/all_params", all_params)
    save_params(f"{sweep_path}/all_params_init", all_params, use_json=True)
    save_params(f"{sweep_path}/all_params_json", all_params, use_json=True)

    with open(f"{sweep_path}/varying_params", "w") as fp:
        yaml.dump(varying_params, fp)

    with open(f"{d_path}/latest", "w") as fp:
        json.dump(sweep_id, fp)

    print(sweep_id)


def get_config_manual_lock(sweep_path, run_id, mark_as_done=False, finish_undone=False):
    lock = FileLock(f"{sweep_path}/all_params.lock")
    # Sleep for a random amount of time to allow other processes to acquire the lock
    time.sleep(np.random.random() * 10)

    with lock:
        if not "all_params" in os.listdir(sweep_path):
            assert "all_params_json" in os.listdir(
                sweep_path
            ), f"Provide list of parameters to sweep on"
            all_configs = load_params(f"{sweep_path}/all_params_json", use_json=True)
            save_params(f"{sweep_path}/all_params", all_configs)

        load, i = True, 0

        while load and i < 10000:
            # Retry loading the file in case of json errors
            all_configs, load = load_params(f"{sweep_path}/all_params")
            i += 1

        if all_configs is None:
            return None, False

        for config in all_configs:
            try:
                if config["run_id"] == run_id and mark_as_done:
                    config["done"] = True
                    save_params(f"{sweep_path}/all_params", all_configs)
                    save_params(
                        f"{sweep_path}/all_params_json", all_configs, use_json=True
                    )
                    return config, False

                elif finish_undone and not config["done"]:
                    config["done"] = "running"
                    config["run_id"] = run_id
                    save_params(f"{sweep_path}/all_params", all_configs)
                    save_params(
                        f"{sweep_path}/all_params_json", all_configs, use_json=True
                    )
                    return config, False

            except KeyError:
                config["run_id"] = run_id
                config["done"] = False
                save_params(f"{sweep_path}/all_params", all_configs)
                save_params(f"{sweep_path}/all_params_json", all_configs, use_json=True)
                # time.sleep(np.random.random() * 2 + 0.1)
                return config, False

    return None, False


if __name__ == "__main__":
    # ----- Define or Load Varyings Params -----

    varying_params = {yaml.load(open("varying_params.yaml", "r"))}

    f_path = os.path.realpath(__file__)
    d_path = os.path.split(f_path)[0]

    generate_sweep(varying_params, d_path)
