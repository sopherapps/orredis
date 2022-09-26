# Ideas

- To reduce on the cloning, and thus make the package faster and more memory-efficient,
  use [slices](https://medium.com/journey-to-rust/nitty-gritty-details-of-char-iterators-4fd4b70b6540)
  or [Box](https://doc.rust-lang.org/book/ch15-01-box.html)
- Boxes can be used to keep huge data on a heap and only copy around its pointers.
- Slices basically use the exact data. The issue with this approach might be that for the conversions I a doing, I need
  to create new instances.
  could help reduce or eliminate the intermediate reallocations during conversion.