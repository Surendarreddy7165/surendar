# # Online Python compiler (interpreter) to run Python online.
# # Write Python 3 code in this online editor and run it.
# print("Try programiz.pro")

# print("SURENDAR")

# word count program

def word_count(input_string):
    words = input_string.split()
    return len(words)
input_string = "RAMANJENEYA REDDY"
print("wordcount:",word_count(input_string))


# character count
def char_occurrence(input_string):
    char_count = {}
    for char in input_string:
        if char in char_count:
            char_count[char] += 1
        else:
            char_count[char] = 1
    return char_count
print("Character Occurence:",char_occurrence(input_string))

# reverse the string

def reverse_string(input_string):
    return input_string[::-1]
print("Reversed String:",reverse_string(input_string))

# combined Examples
input_string = "HAII THIS IS SURENDAR REDDY MARAM"

print("Word Count:",word_count(input_string))

print("Character Occurrence:",char_occurrence(input_string))

print("Reversed String:",reverse_string(input_string))