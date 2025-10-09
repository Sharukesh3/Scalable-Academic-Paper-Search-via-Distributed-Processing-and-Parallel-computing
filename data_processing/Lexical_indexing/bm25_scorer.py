import subprocess
import os
import ctypes
import numpy as np

# --- Configuration ---
CUDA_KERNEL_PATH = "bm25_kernel.cu"
SHARED_LIBRARY_PATH = "./bm25_kernel.so"
COMPILER = "nvcc"

class BM25Scorer:
    """
    A Python wrapper for the custom CUDA BM25 kernel.
    Handles compilation and provides an interface to call the kernel.
    """
    def __init__(self):
        self._compile_kernel()
        self.kernel = self._load_kernel()

    def _compile_kernel(self):
        # Compile the .cu file into a .so shared library if it doesn't exist
        if not os.path.exists(SHARED_LIBRARY_PATH):
            print(f"Compiling CUDA kernel '{CUDA_KERNEL_PATH}'...")
            try:
                subprocess.run(
                    [COMPILER, "-shared", "-o", SHARED_LIBRARY_PATH, CUDA_KERNEL_PATH, "-Xcompiler", "-fPIC"],
                    check=True,
                    capture_output=True,
                    text=True
                )
                print("Compilation successful.")
            except subprocess.CalledProcessError as e:
                print("CUDA compilation failed.")
                print("NVCC stdout:", e.stdout)
                print("NVCC stderr:", e.stderr)
                raise

    def _load_kernel(self):
        # Load the compiled shared library
        lib = ctypes.CDLL(SHARED_LIBRARY_PATH)
        # Define the function signature to match the C++ kernel
        func = lib.score_documents_cuda
        func.argtypes = [
            ctypes.POINTER(ctypes.c_float),  # scores_out
            ctypes.c_int,                   # num_docs
            # Use NumPy's ctypes interface for pointer to float
            np.ctypeslib.ndpointer(dtype=np.int32, flags='C_CONTIGUOUS'),   # query_term_ids
            ctypes.c_int,                   # num_query_terms
            # FIX: Changed C++ comments (//) to Python comments (#)
            np.ctypeslib.ndpointer(dtype=np.int32, flags='C_CONTIGUOUS'),   # doc_lengths
            np.ctypeslib.ndpointer(dtype=np.int32, flags='C_CONTIGUOUS'),   # tfs_indices
            np.ctypeslib.ndpointer(dtype=np.int32, flags='C_CONTIGUOUS'),   # tfs_values
            np.ctypeslib.ndpointer(dtype=np.int32, flags='C_CONTIGUOUS'),   # tfs_indptr
            np.ctypeslib.ndpointer(dtype=np.float32, flags='C_CONTIGUOUS'), # idf_scores
            ctypes.c_float,                 # k1
            ctypes.c_float,                 # b
            ctypes.c_float                  # avg_doc_length
        ]
        return func

    def score(self, query_term_ids, doc_lengths, tfs_matrix, idf_scores, k1, b, avg_doc_length):
        # Prepare data for the kernel call
        num_docs = len(doc_lengths)
        scores_out = np.zeros(num_docs, dtype=np.float32)

        # Ensure all numpy arrays are in the correct C-contiguous format
        query_term_ids = np.ascontiguousarray(query_term_ids, dtype=np.int32)
        doc_lengths = np.ascontiguousarray(doc_lengths, dtype=np.int32)
        tfs_indices = np.ascontiguousarray(tfs_matrix.indices, dtype=np.int32)
        tfs_values = np.ascontiguousarray(tfs_matrix.data, dtype=np.int32)
        tfs_indptr = np.ascontiguousarray(tfs_matrix.indptr, dtype=np.int32)
        idf_scores = np.ascontiguousarray(idf_scores, dtype=np.float32)

        # Call the CUDA function
        self.kernel(
            scores_out.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
            num_docs,
            query_term_ids,
            len(query_term_ids),
            doc_lengths,
            tfs_indices,
            tfs_values,
            tfs_indptr,
            idf_scores,
            k1,
            b,
            avg_doc_length
        )
        return scores_out

