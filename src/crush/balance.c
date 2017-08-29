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

int balance_values(int values_count, int items_count, __s64* original_straws, __u32* target_weights, __u32* weights)
{
  __s64 straws[values_count * items_count];
  for (int i = 0; i < values_count; i++)
    for (int j = 0; j < items_count; j++)
      straws[i * items_count + j] = div64_s64(original_straws[i * items_count + j], weights[j]);
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
  for (int i = 0; i < values_count - total_expected; i++)
    expected[i]++;

  int max_iterations = CRUSH_BALANCE_MAX_ITERATIONS + values_count;
  int iterations;
  for (iterations = 0; iterations < max_iterations; iterations++) {
    int delta[items_count];
    memcpy(delta, expected, sizeof(int) * items_count);

    // One of the values that landed on each item because it
    // got a straw that is not much higher than the second best straw.
    // This is the closest winner. And the closest loser is the item
    // that would have won the value otherwise, i.e. the second best.
    // This is the closest loser.
    double closest_losers[items_count];
    int closest_losers_value[items_count];
    __s64 closest_losers_diff[items_count];
    double closest_winners[items_count];
    int closest_winners_value[items_count];
    __s64 closest_winners_diff[items_count];
    for (int j = 0; j < items_count; j++) {
      closest_losers[j] = DBL_MAX;
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
      int loser_item = -1;
      __s64 loser_straw = S64_MIN;
      __s64 winner_diff = S64_MAX; // by how much did it win
      for (int j = 0; j < items_count; j++) {
        if (j == winner_item)
          continue;
        __s64 maybe_winner_diff = labs(items_straws[winner_item] - items_straws[j]);
        if (maybe_winner_diff < winner_diff) {
          winner_diff = maybe_winner_diff;
          loser_straw = items_straws[j];
          loser_item = j;
        }
      }
      double winner_ratio = (double)winner_diff / labs(loser_straw);
      if (closest_losers[loser_item] > winner_ratio) {
        closest_losers[loser_item] = winner_ratio;
        closest_losers_value[loser_item] = i;
        closest_losers_diff[loser_item] = winner_diff;
      }
      if (closest_winners[winner_item] > winner_ratio) {
        closest_winners[winner_item] = winner_ratio;
        closest_winners_value[winner_item] = i;
        closest_winners_diff[winner_item] = winner_diff;
      }

      // negative delta is overfilled, positive is underfilled
      delta[winner_item] -= 1;
    }

    int highest_variance = 0;
    for (int j = 0; j < items_count; j++) {
      highest_variance = MAX(highest_variance, abs(delta[j]));
      printf("%d ", delta[j]);
    }
    printf("\n");
    // there is little to gain with a perfect distribution, +-1 is
    // good enough
    if (highest_variance <= 1)
      break;

    // find the smallest difference so that we can either remove a value
    // from an overfilled item and add the subtracted weight to an
    // underfilled item or add a value to an overfilled item and sub the
    // added weight from an overfilled item
  
    double smallest_winner_ratio = DBL_MAX;
    int smallest_winner_item = -1;

    double smallest_loser_ratio = DBL_MAX;
    int smallest_loser_item = -1;

    for (int j = 0; j < items_count; j++) {
      if (delta[j] == 0) // balanced
        continue;
      if (delta[j] < 0) {
        // overfilled
        if (closest_winners[j] < smallest_winner_ratio) {
          smallest_winner_item = j;
          smallest_winner_ratio = closest_winners[j];
        }
      } else {
        // underfilled
        if (closest_losers[j] < smallest_loser_ratio) {
          smallest_loser_item = j;
          smallest_loser_ratio = closest_losers[j];
        }
      }
    }

    // that should not happen
    if (smallest_loser_item == -1 || smallest_winner_item == -1)
      break;

    double modify;
    int change;
    int value;
    int item;
    __s64 diff;
    if (smallest_winner_ratio < smallest_loser_ratio) {
      item = smallest_winner_item;
      value = closest_winners_value[smallest_winner_item];
      diff = closest_winners_diff[smallest_winner_item];
      modify = closest_winners[smallest_winner_item];
      change = -1; // we need to loose
    } else {
      item = smallest_loser_item;
      value = closest_losers_value[smallest_loser_item];
      diff = closest_losers_diff[smallest_loser_item];
      modify = closest_losers[smallest_loser_item];
      change = 1; // we need to win
    }

    if (closest_winners_value[smallest_winner_item] != closest_losers_value[smallest_loser_item]) {
      // if they are not the same we need to check for rounding errors to make sure a value will
      // be lost/gained where intended
      __s64 desired_straw = straws[value] + change * diff;
      __s64 original_straw = original_straws[value];
      double step = 1.0 - 1 / U32_MAX;
      do {
        __u32 weight = weights[item] * (1.0 + change * modify);
        __s64 straw = div64_s64(original_straw, weight);
        if (labs(straw - desired_straw) > diff)
          break;
        modify *= step;
      } while (1);
    } else {
      // the items switch places for this value, the winner will become the second best and the second best becomes the winner
    }

    weights[smallest_winner_item] *= 1.0 - modify;
    weights[smallest_loser_item] *= 1.0 + modify;

    for (int i = 0; i < values_count; i++) {
      __s64* items_straws = straws + items_count * i;
      __s64* items_original_straws = original_straws + items_count * i;
      items_straws[smallest_winner_item] = div64_s64(items_original_straws[smallest_winner_item], weights[smallest_winner_item]);
      items_straws[smallest_loser_item] = div64_s64(items_original_straws[smallest_loser_item], weights[smallest_loser_item]);
    }
  }
  return iterations;
}
