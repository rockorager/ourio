// This code is mostly a copy of the intrusive queue code from libxev. I've modified it to be a
// doubly linked list that also ensures a certain state is set on each node when put into the list
//
// MIT License
//
// Copyright (c) 2023 Mitchell Hashimoto
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
// associated documentation files (the "Software"), to deal in the Software without restriction,
// including without limitation the rights to use, copy, modify, merge, publish, distribute,
// sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or
// substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
// NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

const std = @import("std");
const assert = std.debug.assert;

/// An intrusive queue implementation. The type T must have a field
/// "next" of type `?*T` and a field "state" which is an enum with a value matching the passed in
/// value
pub fn Intrusive(comptime T: type, comptime state: @Type(.enum_literal)) type {
    return struct {
        const Self = @This();

        const set_state = state;

        /// Head is the front of the queue and tail is the back of the queue.
        head: ?*T = null,
        tail: ?*T = null,

        /// Enqueue a new element to the back of the queue.
        pub fn push(self: *Self, v: *T) void {
            assert(v.next == null);
            v.state = set_state;

            if (self.tail) |tail| {
                // If we have elements in the queue, then we add a new tail.
                tail.next = v;
                v.prev = tail;
                self.tail = v;
            } else {
                // No elements in the queue we setup the initial state.
                self.head = v;
                self.tail = v;
            }
        }

        /// Dequeue the next element from the queue.
        pub fn pop(self: *Self) ?*T {
            // The next element is in "head".
            const next = self.head orelse return null;

            // If the head and tail are equal this is the last element
            // so we also set tail to null so we can now be empty.
            if (self.head == self.tail) self.tail = null;

            // Head is whatever is next (if we're the last element,
            // this will be null);
            self.head = next.next;
            if (self.head) |head| head.prev = null;

            // We set the "next" field to null so that this element
            // can be inserted again.
            next.next = null;
            next.prev = null;
            return next;
        }

        /// Returns true if the queue is empty.
        pub fn empty(self: Self) bool {
            return self.head == null;
        }

        /// Removes the item from the queue. Asserts that Queue contains the item
        pub fn remove(self: *Self, item: *T) void {
            assert(self.hasItem(item));
            if (item.prev) |prev| prev.next = item.next else self.head = item.next;

            if (item.next) |next| next.prev = item.prev else self.tail = item.prev;

            item.prev = null;
            item.next = null;
        }

        pub fn hasItem(self: Self, item: *T) bool {
            var maybe_node = self.head;
            while (maybe_node) |node| {
                if (node == item) return true;
                maybe_node = node.next;
            } else return false;
        }

        pub fn len(self: Self) usize {
            var count: usize = 0;
            var maybe_node = self.head;
            while (maybe_node) |node| {
                count += 1;
                maybe_node = node.next;
            }
            return count;
        }
    };
}
