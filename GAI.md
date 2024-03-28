# GEN AI P4 Chatbot Feedback

### Example: Describe the bot UI Design that we had to use because of Maizey limitations. {: #Q01}
- [ ] Yum
- [x] Yuk

## Background

### How many ULCS courses has group member 1 taken including the current semester? {: #Q02}
4
### How many ULCS courses has group member 2 taken including the current semester? (0 if you are working alone) {: #Q03}
1
### How many ULCS courses has group member 3 taken including the current semester? (0 if you are working alone or in a group of 2) {: #Q04}
2

## General
### Approximately how many interactions did you have with the bot on project 4? {: #Q05}
- [ ] 0-3
- [x] 4-10
- [ ] 11-50
- [ ] 51+

### Mark all the topics where the bot was helpful: {: #Q06}
- [ ] Spec questions
- [ ] Design
- [ ] Debugging
- [x] Conceptual questions
- [ ] Explaining test cases
- [ ] Setup / toolchain
- [ ] Implementation / code suggestions

### Would a similar bot be helpful for other projects? {: #Q07}
- [x] Yes
- [ ] No
- [ ] Unsure

### Our group used the bot instead of posting on Piazza {: #Q08}
- [ ] Never
- [x] Rarely
- [ ] Sometimes
- [ ] Often
- [ ] Always

### Our group used the bot instead of going to Office Hours {: #Q09}
- [ ] Never
- [x] Rarely
- [ ] Sometimes
- [ ] Often
- [ ] Always

### The bot gave more helpful answers than ChatGPT {: #Q10}
(Reminder: Course policy permits the use of generative AI for projects. https://eecs485.org/syllabus.html#generative-ai-copilot-chatgpt)
- [ ] Never
- [ ] Rarely
- [x] Sometimes
- [ ] Often
- [ ] Always
- [ ] I did not use ChatGPT

### Tell us about your experience using our bot instead of ChatGPT. {: #Q11}
For example, I didn't know how many threads should we use for this project. I just asked Mappy and it gave me some suggestions.

#### (Optional) Overall, do you have any feedback on the bot? {: #Q12}
Good, but kinda slow response.

## Interaction 1
### Prompt {: #Q13}
Can worker receive and send the message in a single thread?

### Response {: #Q14}
Yes, a worker can both receive and send messages in a single thread; it just needs to be designed in such a way that it can handle incoming messages (like a shutdown command) while it is processing a task. This generally involves async I/O operations in the thread, enabling the worker to check for new messages periodically during its execution of a task, or in between tasks if the processing is broken up into smaller sub-tasks.


One approach to handle this in a synchronous environment would be to have a loop that alternates between checking for incoming messages and performing the task work; however, special care must be taken to ensure that the task work does not block for so long that it would delay the handling of incoming messages significantly.

### Was this response helpful? {: #Q15}
- [x] Yes
- [ ] No

### To the best of your knowledge, did it answer your question correctly: {: #Q16}
- [x] Yes
- [ ] No
- [ ] Unsure

### (Optional) Anything else you want to tell us about this interaction? {: #Q17}
FIXME

## Interaction 2
### Prompt {: #Q18}
If there is no current executing MapReduce job, then pop a job off the queue and execute it. The Manager runs each job to completion before starting a new job. Is this correct?

### Response {: #Q19}
So, if there's an executing job, the Manager will focus on completing that before popping off the next job from the queue. While the active job is being executed, the Manager wouldn't start any other jobs, but it could perform preparatory actions like partitioning inputs for subsequent jobs if the workflow is designed to allow for any such multitasking or pre-processing. However, the specific behavior would depend on the instructions given in the course materials and the design specifications of the MapReduce implementation.



### Was this response helpful? {: #Q20}
- [x] Yes
- [ ] No

### To the best of your knowledge, did it answer your question correctly: {: #Q21}
- [x] Yes
- [ ] No
- [ ] Unsure

### (Optional) Anything else you want to tell us about this interaction? {: #Q22}
FIXME

## Interaction 3
### Prompt {: #Q23}
I am wondering what is the output for one file in one worker after runing the map executable

### Response {: #Q24}
After running the map executable on one input file in a worker, the output typically consists of key-value pairs. Each line in the output usually represents one key-value pair, formatted so that it can be consumed by the following stages in the MapReduce process.


For example, if the input file was a text file containing words, after running the map executable specifically designed to count words, the resulting output for the file might be something like this:


word1 1
word2 1
word1 1
word3 1
...

In this output, "word1", "word2", and "word3" are the keys, and the '1's represent the value assigned to each occurrence of the word. This is an intermediate step in the MapReduce workflow. The worker will need to write these key-value pairs into the proper partitions, often based on the key to aid in the shuffling and reducing phases that come later.

### Was this response helpful? {: #Q25}
- [x] Yes
- [ ] No

### To the best of your knowledge, did it answer your question correctly: {: #Q26}
- [x] Yes
- [ ] No
- [ ] Unsure

### (Optional) Anything else you want to tell us about this interaction? {: #Q27}
FIXME
