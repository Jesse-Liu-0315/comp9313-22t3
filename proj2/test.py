sample = ['council', 'chief', 'executive', 'fails', 'to', 'secure', 'position']
list1 = ['to']
for item in sample:
    if item in list1:
        sample.remove(item)
print(sample)