"""Taller evaluable"""

import glob
import os
import shutil
import string
import time


def prepate_input_dicrectory(path: str):
    """Crea la carpeta si no existe o la limpia si ya existe."""
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
    else:
        for file in glob.glob(f"{path}/*"):
            os.remove(file)


def copy_raw_files_to_input_folder(n, raw_path="files/raw", input_path="files/input"):
    """Copia los archivos raw n veces al input."""
    prepate_input_dicrectory(input_path)

    for file in glob.glob(f"{raw_path}/*"):
        with open(file, "r", encoding="utf-8") as f:
            content = f.read()

        for i in range(n):
            raw_file_name = os.path.basename(file)
            raw_file_name_without_ext, raw_file_ext = os.path.splitext(raw_file_name)

            with open(
                f"{input_path}/{raw_file_name_without_ext}_{i}.{raw_file_ext.lstrip('.')}",
                "w",
                encoding="utf-8",
            ) as out_file:
                out_file.write(content)


def mapper(sequence):
    pairs_sequence = []
    for _, line in sequence:
        line = line.lower()
        line = line.translate(str.maketrans("", "", string.punctuation))
        line = line.replace("\n", "")
        words = line.split()
        pairs_sequence.extend([(word, 1) for word in words])
    return pairs_sequence


def reducer(pairs_sequence):
    result = []
    for key, value in pairs_sequence:
        if result and result[-1][0] == key:
            result[-1] = (key, result[-1][1] + value)
        else:
            result.append((key, value))
    return result


def hadoop(mapper_func, reducer_func, input_path, output_path):
    """Simula el comportamiento de Hadoop Streaming."""

    def emit_input_lines(input_path):
        sequence = []
        files = glob.glob(f"{input_path}/*")
        for file in files:
            with open(file, "r", encoding="utf-8") as f:
                for line in f:
                    sequence.append((file, line))
        return sequence

    def create_output_folder(output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        else:
            raise FileExistsError(f"The folder {output_dir} already exists")

    def shuffle_and_sort(pairs_sequence):
        return sorted(pairs_sequence)

    def export_results_to_file(output_path, result):
        with open(f"{output_path}/part-00000", "w", encoding="utf-8") as f:
            for key, value in result:
                f.write(f"{key}\t{value}\n")

    def create_success_file(output_path):
        with open(f"{output_path}/_SUCCESS", "w", encoding="utf-8") as f:
            f.write("")

    sequence = emit_input_lines(input_path)
    pairs_sequence = mapper_func(sequence)
    pairs_sequence = shuffle_and_sort(pairs_sequence)
    result = reducer_func(pairs_sequence)

    create_output_folder(output_path)
    export_results_to_file(output_path, result)
    create_success_file(output_path)


def run_job(input_path, output_path):
    """Ejecuta el job de word count."""

    start_time = time.time()

    # Elimina output si ya existe (evita error del autograder)
    if os.path.exists(output_path):
        shutil.rmtree(output_path)

    hadoop(mapper, reducer, input_path, output_path)

    print(f"Tiempo de ejecución: {time.time() - start_time} segundos")


if __name__ == "__main__":
    copy_raw_files_to_input_folder(n=1000)
    run_job("files/input", "files/output")