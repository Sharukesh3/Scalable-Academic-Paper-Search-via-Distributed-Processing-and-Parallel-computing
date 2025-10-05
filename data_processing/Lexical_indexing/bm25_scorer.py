import os
import subprocess
import numpy as np
import ctypes

# Configuration
CUDA_KERNEL_PATH = "bm25_kernel.cu"
SHARED_LIBRARY_PATH = "lib_bm25_kernel.so"

class BM25Scorer:
    """
    A class to compile a CUDA kernel, load it, and provide a Python interface
    for calculating BM25 scores on the GPU.
    """
    def __init__(self):
        self._compile_kernel()
        self._load_library()

    def _compile_kernel(self):
        """Compiles the CUDA kernel into a shared library if it doesn't exist."""
        if not os.path.exists(SHARED_LIBRARY_PATH):
            print(f"Compiling CUDA kernel '{CUDA_KERNEL_PATH}'...")
            try:
                subprocess.run(
                    [
                        "nvcc", "-Xcompiler", "-fPIC", "-shared",
                        "-o", SHARED_LIBRARY_PATH, CUDA_KERNEL_PATH
                    ],
                    check=True,
                    capture_output=True,
                    text=True
                )
                print("Compilation successful.")
            except subprocess.CalledProcessError as e:
                print("CUDA compilation failed!")
                print("NVCC STDOUT:", e.stdout)
                print("NVCC STDERR:", e.stderr)
                raise

    def _load_library(self):
        """Loads the compiled shared library and defines the kernel function signature."""
        self.lib = ctypes.CDLL(SHARED_LIBRARY_PATH)
        self.kernel_func = self.lib.bm25_kernel
        
        # Define the argument types for the C++ function
        self.kernel_func.argtypes = [
            ctypes.c_void_p,  # docs_terms
            ctypes.c_void_p,  # doc_offsets
            ctypes.c_void_p,  // doc_lengths
            ctypes.c_void_p,  # query_term_ids
            ctypes.c_void_p,  # idf_scores
            ctypes.c_int,     # num_docs
            ctypes.c_int,     # num_query_terms
            ctypes.c_float,   # avg_doc_length
            ctypes.c_float,   # k1
            ctypes.c_float,   # b
            ctypes.c_void_p   # out_scores
        ]

    def score_batch(self, docs_terms, doc_offsets, doc_lengths, query_term_ids, idf_scores, 
                    avg_doc_length, k1=1.5, b=0.75):
        """
        Calculates BM25 scores for a batch of documents on the GPU.
        
        All array inputs are expected to be NumPy arrays.
        """
        num_docs = len(doc_lengths)
        num_query_terms = len(query_term_ids)
        
        # Create an empty NumPy array to hold the output scores
        out_scores = np.zeros(num_docs, dtype=np.float32)

        # Call the CUDA kernel via ctypes
        self.kernel_func(
            docs_terms.ctypes.data,
            doc_offsets.ctypes.data,
            doc_lengths.ctypes.data,
            query_term_ids.ctypes.data,
            idf_scores.ctypes.data,
            num_docs,
            num_query_terms,
            avg_doc_length,
            k1,
            b,
            out_scores.ctypes.data
        )
        return out_scores

# Example usage (for testing)
if __name__ == '__main__':
    # This is a dummy test case to ensure compilation and calling works.
    print("Testing BM25 Scorer...")
    scorer = BM25Scorer()
    
    # Mock data for one document
    # Doc: contains term 10 (tf=2) and term 25 (tf=1)
    docs_terms = np.array([(10, 2.0), (25, 1.0)], dtype=[('id', 'i4'), ('tf', 'f4')])
    doc_offsets = np.array([0, 2], dtype=np.int32) # Doc 0 starts at index 0 and ends at index 2
    doc_lengths = np.array([15], dtype=np.int32) # Doc 0 has length 15
    
    # Query: for terms 10 and 30
    query_term_ids = np.array([10, 30], dtype=np.int32)
    idf_scores = np.array([0.5, 1.2], dtype=np.float32) # IDF for term 10 and 30
    
    avg_doc_length = 20.0
    
    scores = scorer.score_batch(docs_terms, doc_offsets, doc_lengths, query_term_ids, idf_scores, avg_doc_length)
    
    print("\nTest completed.")
    print("Output scores:", scores)
    # The score should be non-zero, calculated only from term 10, since term 30 is not in the doc.
