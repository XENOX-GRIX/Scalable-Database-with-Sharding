# Sample dictionary
my_dict = {
    'key1': [1, 2, 3],
    'key2': [4, 5],
    'key3': [6, 7, 8, 9],
    'key4': [10]
}

# Sort the dictionary based on the length of the value list in decreasing order
sorted_dict = dict(sorted(my_dict.items(), key=lambda x: len(x[1]), reverse=True))

# Print the sorted dictionary
print(sorted_dict)