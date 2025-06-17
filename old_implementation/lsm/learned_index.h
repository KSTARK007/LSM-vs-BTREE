#ifndef LEARNED_INDEX_H
#define LEARNED_INDEX_H

#include <vector>
#include <string> // For KeyType if it's string, adjust as needed
#include <cstdint> // For uint64_t if KeyType is numeric

// Assuming KeyType is defined globally (e.g., in your "global.h" or similar)
// If not, you might need to include that header or define KeyType here.
// For example:
// #include "global.h"
using KeyType = uint64_t; // Placeholder: Replace with your actual KeyType

#include "global.h" // Include our new config file

class LearnedIndex {
private:
    struct SegmentModel {
        KeyType first_key;          // The first key this model is responsible for (inclusive).
        double slope;
        double intercept;
        double max_abs_error;       // Max absolute error for this segment's model.
        size_t start_index_global;  // The global index in the original sorted_keys array that corresponds to this segment's first_key.
        size_t num_keys_in_segment; // Number of keys this segment was trained on.

        SegmentModel(KeyType fk, double s, double i, double mae, size_t sig, size_t nk)
            : first_key(fk), slope(s), intercept(i), max_abs_error(mae), start_index_global(sig), num_keys_in_segment(nk) {}
    };

    std::vector<SegmentModel> segments_;
    bool model_trained_ = false;
    KeyType min_overall_key_;
    KeyType max_overall_key_;
    size_t total_keys_trained_on_ = 0;

    // Helper for training individual segments
    static bool train_linear_model(const KeyType* keys, const size_t* global_indices, size_t num_keys,
                                   double& out_slope, double& out_intercept, double& out_max_abs_error);

public:
    LearnedIndex();

    // Trains the piecewise linear model on the provided sorted keys.
    void train(const std::vector<KeyType>& sorted_keys);

    // Predicts the potential range of sorted indices [effective_min_idx, effective_max_idx] for the key.
    // Returns true if a prediction can be made (even if range is empty).
    // `total_entry_count` is the total number of entries the index was built upon.
    bool predict_index_range(KeyType key,
                               int& effective_min_idx, int& effective_max_idx) const;

    bool is_trained() const { return model_trained_; }
    KeyType get_min_training_key() const { return min_overall_key_; }
    KeyType get_max_training_key() const { return max_overall_key_; }
};
#endif // LEARNED_INDEX_H
