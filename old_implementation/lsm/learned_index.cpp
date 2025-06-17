#include "learned_index.h"
#include <algorithm> // For std::sort, std::min, std::max, std::upper_bound
#include <cmath>     // For std::abs, std::round, std::ceil, std::floor, std::sqrt
#include <vector>
#include <limits>    // For std::numeric_limits

LearnedIndex::LearnedIndex()
    : model_trained_(false), 
      min_overall_key_(std::numeric_limits<KeyType>::min()), // Or appropriate init for your KeyType
      max_overall_key_(std::numeric_limits<KeyType>::max()), // Or appropriate init
      total_keys_trained_on_(0) {}

bool LearnedIndex::train_linear_model(const KeyType* keys, const size_t* global_indices, size_t num_keys,
                                      double& out_slope, double& out_intercept, double& out_max_abs_error) {
    if (num_keys < LEARNED_INDEX_MIN_KEYS_PER_SEGMENT_TRAINING && num_keys > 0) { // Allow 1 key as special case
         // Simplified handling for very few keys:
        if (num_keys == 1) {
            out_slope = 0.0;
            out_intercept = static_cast<double>(global_indices[0]);
            out_max_abs_error = 0.0;
            return true;
        }
        // else if fewer than min threshold but > 1, could try to fit, or mark as poor quality.
        // For simplicity, we proceed but the model might not be great.
        // A more robust system might merge small segments.
    } else if (num_keys == 0) {
        return false;
    }


    KeyType first_key_val = keys[0];
    KeyType last_key_val = keys[num_keys - 1];

    if (num_keys == 1 || first_key_val == last_key_val) { // All keys in segment are the same or only one key
        out_slope = 0.0;
        // Predict the average of global indices for these identical keys
        long double sum_global_indices = 0;
        for(size_t i = 0; i < num_keys; ++i) sum_global_indices += global_indices[i];
        out_intercept = static_cast<double>(sum_global_indices / num_keys);
        
        out_max_abs_error = 0.0;
        for (size_t i = 0; i < num_keys; ++i) {
            double predicted_idx = out_intercept; // Since slope is 0
            out_max_abs_error = std::max(out_max_abs_error, std::abs(predicted_idx - static_cast<double>(global_indices[i])));
        }
        return true;
    }

    long double sum_x = 0, sum_y = 0, sum_xy = 0, sum_x_sq = 0;
    for (size_t i = 0; i < num_keys; ++i) {
        sum_x += static_cast<long double>(keys[i]);
        sum_y += static_cast<long double>(global_indices[i]);
        sum_xy += static_cast<long double>(keys[i]) * static_cast<long double>(global_indices[i]);
        sum_x_sq += static_cast<long double>(keys[i]) * static_cast<long double>(keys[i]);
    }

    long double N_ld = static_cast<long double>(num_keys);
    long double slope_denominator = (N_ld * sum_x_sq - sum_x * sum_x);

    if (std::abs(slope_denominator) < 1e-12) { // Denominator is too small, avoid division by zero
        // Fallback: Horizontal line through average y (global_index)
        out_slope = 0.0;
        out_intercept = static_cast<double>(sum_y / N_ld);
    } else {
        out_slope = static_cast<double>((N_ld * sum_xy - sum_x * sum_y) / slope_denominator);
        out_intercept = static_cast<double>((sum_y - static_cast<long double>(out_slope) * sum_x) / N_ld);
    }

    out_max_abs_error = 0.0;
    for (size_t i = 0; i < num_keys; ++i) {
        double predicted_idx = out_slope * static_cast<double>(keys[i]) + out_intercept;
        out_max_abs_error = std::max(out_max_abs_error, std::abs(predicted_idx - static_cast<double>(global_indices[i])));
    }
    return true;
}


void LearnedIndex::train(const std::vector<KeyType>& sorted_keys) {
    model_trained_ = false;
    segments_.clear();
    total_keys_trained_on_ = sorted_keys.size();

    if (sorted_keys.empty()) {
        return;
    }

    min_overall_key_ = sorted_keys.front();
    max_overall_key_ = sorted_keys.back();

    size_t num_total_keys = sorted_keys.size();
    std::vector<size_t> global_indices(num_total_keys);
    for(size_t i=0; i < num_total_keys; ++i) global_indices[i] = i;

    size_t num_segments;
    if (num_total_keys < LEARNED_INDEX_MIN_KEYS_FOR_MULTISEGMENT) {
        num_segments = 1;
    } else {
        num_segments = (num_total_keys + LEARNED_INDEX_TARGET_KEYS_PER_SEGMENT - 1) / LEARNED_INDEX_TARGET_KEYS_PER_SEGMENT;
        num_segments = std::max(static_cast<size_t>(1), num_segments); // Ensure at least one segment
    }

    for (size_t i = 0; i < num_segments; ++i) {
        size_t segment_start_offset = (i * num_total_keys) / num_segments;
        size_t segment_end_offset = ((i + 1) * num_total_keys) / num_segments;
        if (i == num_segments - 1) { // Ensure last segment goes to the end
            segment_end_offset = num_total_keys;
        }
        
        size_t current_segment_num_keys = segment_end_offset - segment_start_offset;

        if (current_segment_num_keys == 0) continue; // Should not happen if num_total_keys > 0

        KeyType segment_first_key = sorted_keys[segment_start_offset];
        size_t segment_start_global_idx = global_indices[segment_start_offset];

        double s_slope, s_intercept, s_max_abs_error;
        if (train_linear_model(&sorted_keys[segment_start_offset], 
                               &global_indices[segment_start_offset], 
                               current_segment_num_keys,
                               s_slope, s_intercept, s_max_abs_error)) {
            segments_.emplace_back(segment_first_key, s_slope, s_intercept, s_max_abs_error, segment_start_global_idx, current_segment_num_keys);
        } else {
            // Failed to train a model for this segment (e.g. 0 keys, though guarded)
            // This indicates an issue in segmentation logic or very sparse data.
            // For robustness, could skip this segment or use a dummy model.
            // Here, we just don't add it, which might lead to gaps if not handled in predict.
        }
    }
    
    if (!segments_.empty()) {
        model_trained_ = true;
    }
}

bool LearnedIndex::predict_index_range(KeyType key,
                                       int& effective_min_idx, int& effective_max_idx) const {
    if (!model_trained_ || segments_.empty() || total_keys_trained_on_ == 0) {
        effective_min_idx = 0; // Default to full range if not trained, or invalid
        effective_max_idx = static_cast<int>(total_keys_trained_on_ > 0 ? total_keys_trained_on_ - 1 : 0);
        return false; // Cannot make a specific prediction
    }

    // Key is outside the overall range the model was trained on.
    // The SSTable's min/max key check should ideally catch this before calling.
    // If called, means extrapolation. For filtering, safer to predict empty or not predict.
    if (key < min_overall_key_ || key > max_overall_key_) {
         // To be safe for filtering, if key is out of overall bounds, predict an empty range.
        effective_min_idx = 1; 
        effective_max_idx = 0; 
        return true; // Prediction made (it's an empty range)
    }
    
    const SegmentModel* chosen_segment = nullptr;

    // Find the segment responsible for this key
    // std::upper_bound returns iterator to the first element > key (or value that makes lambda true)
    auto it = std::upper_bound(segments_.begin(), segments_.end(), key,
        [](KeyType k_val, const SegmentModel& seg_model) {
            return k_val < seg_model.first_key;
        });

    if (it == segments_.begin()) {
        // Key is <= first_key of the first segment. It must be the first segment.
        chosen_segment = &segments_.front();
    } else {
        // 'it' points to the first segment whose first_key > key.
        // So, the correct segment is the one before 'it'.
        chosen_segment = &(*(it - 1));
    }
    
    // If somehow no segment chosen (should not happen if segments_ is not empty and key is within overall range)
    if (!chosen_segment) {
         effective_min_idx = 1; 
         effective_max_idx = 0;
         return true; // Cannot find segment, predict empty
    }

    double predicted_idx_double = chosen_segment->slope * static_cast<double>(key) + chosen_segment->intercept;
    double error_bound = chosen_segment->max_abs_error;

    double lower_bound_on_i = predicted_idx_double - error_bound;
    double upper_bound_on_i = predicted_idx_double + error_bound;

    effective_min_idx = static_cast<int>(std::ceil(std::max(0.0, lower_bound_on_i)));
    effective_max_idx = static_cast<int>(std::floor(std::min(static_cast<double>(total_keys_trained_on_ - 1), upper_bound_on_i)));
    
    return true;
}