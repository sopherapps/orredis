# Learnings

- When creating a rust-based python package, avoid thinking about the solution in a pythonic way but rather in a
  language-agnostic way. e.g. don't think about class variables, class methods and such stuff because in the real-world,
  we don't have those.
  If object-oriented design is supposed to mimic the real world, it failed in that regard. Only an instance of something
  can do (i.e. have methods) or have a given property, not a specification of such instances. e.g. a man eats, if there
  was a specification of what a man can be, but no man existed, there would be no eating.
- The reason why you should avoid the above pythonic way of thinking is much as it simplifies using the package, writing
  such an API is very difficult to pull-off while ensuring faster and more efficient operation for the package.
- The other reason is you will find yourself having to do less cloning (thus more memory and CPU efficiency) all in the
  name of trying to keep the borrow-checker happy.
- If you wish to add threading to the rust side, make sure the code to be run in the thread never acquires the GIL lock
  or else you will most likely have a deadlock
- When benchmarking, first run the optimized build i.e. `maturin develop -r` otherwise your code will seem to be slower
  than it actually is.
