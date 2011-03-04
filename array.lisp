;; Copyright (c) 2011 Phil Hargett

;; Permission is hereby granted, free of charge, to any person obtaining a copy
;; of this software and associated documentation files (the "Software"), to deal
;; in the Software without restriction, including without limitation the rights
;; to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
;; copies of the Software, and to permit persons to whom the Software is
;; furnished to do so, subject to the following conditions:

;; The above copyright notice and this permission notice shall be included in
;; all copies or substantial portions of the Software.

;; THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
;; IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
;; FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
;; AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
;; LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
;; OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
;; THE SOFTWARE.

(in-package :hh-redblack)

;; =====================================================================================================================
;;
;; implementation of persistent red-black tree using object array
;;
;; ---------------------------------------------------------------------------------------------------------------------

;; ---------------------------------------------------------------------------------------------------------------------
;; types
;; ---------------------------------------------------------------------------------------------------------------------

(defclass memory-persistent-red-black-tree (persistent-red-black-tree)
  ((objects :initform (make-array 0 :adjustable t :fill-pointer t) :accessor objects)))

;; ---------------------------------------------------------------------------------------------------------------------
;; implementation : In-memory storage -- treats a vector as storage, with indexes as locations
;; ---------------------------------------------------------------------------------------------------------------------

(defun make-memory-persistent-red-black-tree () ;; TODO consider having an argument for the tree class
  (let ((tree nil))
    (with-rb-transaction ((setf tree  (make-instance 'memory-persistent-red-black-tree)))
      tree)))

(defmethod prb-open-storage ((tree memory-persistent-red-black-tree))
  )

(defmethod prb-stash-node ((tree memory-persistent-red-black-tree) left-location right-location color-value key-value data-location)
  (vector-push-extend (list left-location right-location color-value key-value data-location) (objects tree)))

(defmethod prb-fetch-node ((tree memory-persistent-red-black-tree) location)
  (destructuring-bind (left right color key data) (aref (objects tree) location)
    (values left right color key data)))

(defmethod prb-fetch-data ((tree memory-persistent-red-black-tree) location)
  (aref (objects tree) location))

(defmethod prb-stash-data ((tree memory-persistent-red-black-tree) data)
  (vector-push-extend data (objects tree)))

(defmethod prb-close-storage ((tree memory-persistent-red-black-tree)))

(defmethod prb-location ((tree memory-persistent-red-black-tree))
  (length (objects tree)))

(defmethod prb-leaf-location-p ((tree memory-persistent-red-black-tree) location)
  (= 0 location))

(defmethod prb-save-root ((tree memory-persistent-red-black-tree) root)
  ;; must mark as unloaded so that it behaves properly in next transaction
  (setf (state root) :unloaded)
  (setf (slot-value tree 'root) root))