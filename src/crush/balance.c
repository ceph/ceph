#include <assert.h>
#include <float.h>
#include <memory.h>

#include "crush_compat.h"
#include "int_types.h"

#include "balance.h"

#ifndef MAX
#define MAX(a, b) ((a) > (b) ? (a) : (b))
#endif // MAX

#ifndef MIN
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#endif // MIN

int balance_values(int values_count, int items_count, __s64* straws, __u32* target_weights)
{
  // a single item is always perfectly balanced
  if (items_count < 2)
    return 0;

  // how many values are expected for each item ?
  __u64 total_weight = 0;
  for (int i = 0; i < items_count; i++)
    total_weight += target_weights[i];
  int expected[items_count];
  int total_expected = 0;
  for (int i = 0; i < items_count; i++) {
    double normalized_weight = (double)target_weights[i] / total_weight;
    expected[i] = values_count * normalized_weight;
    total_expected += expected[i];
  }
  assert(values_count - total_expected < items_count);

  int max_iterations = 1000 + values_count;
  int iterations;
  for (iterations = 0; iterations < max_iterations; iterations++) {
    int delta[items_count];
    memcpy(delta, expected, sizeof(int) * items_count);

    // One of the values that landed on each item because it
    // got a straw that is not much higher than the second best straw.
    // This is the closest winner. And the closest looser is the item
    // that would have won the value otherwise, i.e. the second best.
    // This is the closest looser.
    double closest_loosers[items_count];
    double closest_winners[items_count];
    for (int j = 0; j < items_count; j++) {
      closest_loosers[j] = DBL_MAX;
      closest_winners[j] = DBL_MAX;
    }
  
    for (int i = 0; i < values_count; i++) {
      __s64* items_straws = straws + items_count * i;
      int winner_item = -1;
      __s64 winner_straw = S64_MIN;
      for (int j = 0; j < items_count; j++) {
        if (items_straws[j] > winner_straw) {
          winner_straw = items_straws[j];
          winner_item = j;
        }
      }
      int looser_item = -1;
      __s64 winner_diff = S64_MAX; // by how much did the winner won
      for (int j = 0; j < items_count; j++) {
        if (j == winner_item)
          continue;
        __s64 maybe_winner_diff = items_straws[winner_item] - items_straws[j];
        if (maybe_winner_diff < winner_diff) {
          winner_diff = maybe_winner_diff;
          looser_item = j;
        }
      }
      double winner_ratio = (double)winner_diff / winner_straw;
      if (closest_loosers[looser_item] > winner_ratio)
        closest_loosers[looser_item] = winner_ratio;
      if (closest_winners[winner_item] > winner_ratio)
        closest_winners[winner_item] = winner_ratio;

      // negative delta is overfilled, positive is underfilled
      delta[winner_item] -= 1;
    }

}
