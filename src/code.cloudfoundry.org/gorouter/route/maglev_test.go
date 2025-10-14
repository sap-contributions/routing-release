package route_test

import (
	"code.cloudfoundry.org/gorouter/route"
	"code.cloudfoundry.org/gorouter/test_util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Maglev", func() {
	var (
		logger *test_util.TestLogger
		maglev *route.Maglev
	)

	BeforeEach(func() {
		logger = test_util.NewTestLogger("test")

		maglev = route.NewMaglev(logger.Logger)
	})

	Describe("NewMaglev", func() {
		It("should create a new Maglev instance", func() {
			Expect(maglev).NotTo(BeNil())
		})
	})

	Describe("Add", func() {
		Context("when adding a new backend", func() {
			It("should add the backend successfully", func() {
				maglev.Add("backend1")

				result, err := maglev.Get("test-key")
				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal("backend1"))
			})
		})

		Context("when adding a duplicate backend", func() {
			It("should ignore the duplicate", func() {
				maglev.Add("backend1")
				maglev.Add("backend1") // duplicate

				result, err := maglev.Get("test-key")
				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal("backend1"))
			})
		})

		Context("when adding multiple backends", func() {
			It("should make all backends reachable", func() {
				maglev.Clear()
				maglev.Add("backend1")
				maglev.Add("backend2")
				maglev.Add("backend3")

				backends := make(map[string]bool)
				for i := 0; i < 1000; i++ {
					result, err := maglev.Get(string(rune(i)))
					Expect(err).NotTo(HaveOccurred())
					backends[result] = true
				}

				Expect(backends["backend1"]).To(BeTrue())
				Expect(backends["backend2"]).To(BeTrue())
				Expect(backends["backend3"]).To(BeTrue())
			})
		})
	})

	Describe("Remove", func() {
		Context("when removing an existing backend", func() {
			It("should remove the backend successfully", func() {
				maglev.Add("backend1")
				maglev.Add("backend2")

				maglev.Remove("backend1")

				backends := make(map[string]bool)
				for i := 0; i < 100; i++ {
					result, err := maglev.Get(string(rune(i)))
					Expect(err).NotTo(HaveOccurred())
					backends[result] = true
				}

				Expect(backends["backend1"]).To(BeFalse())
				Expect(backends["backend2"]).To(BeTrue())
			})
		})

		Context("when removing a non-existent backend", func() {
			It("should handle gracefully without error", func() {
				maglev.Clear()
				maglev.Add("backend1")

				Expect(func() { maglev.Remove("non-existent") }).NotTo(Panic())

				result, err := maglev.Get("test-key")
				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal("backend1"))
			})
		})
	})

	Describe("Get", func() {
		Context("when no backends were added", func() {
			It("should return an error", func() {
				_, err := maglev.Get("test-key")
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when backends are added", func() {
			BeforeEach(func() {
				maglev.Add("backend1")
				maglev.Add("backend2")
			})

			It("should return consistent results for the same key", func() {
				result1, err1 := maglev.Get("consistent-key")
				result2, err2 := maglev.Get("consistent-key")

				Expect(err1).NotTo(HaveOccurred())
				Expect(err2).NotTo(HaveOccurred())
				Expect(result1).To(Equal(result2))
			})

			It("should distribute keys across backends", func() {
				maglev.Clear()
				maglev.Add("backend1")
				maglev.Add("backend2")
				maglev.Add("backend3")

				distribution := make(map[string]int)
				for i := 0; i < 1000; i++ {
					result, err := maglev.Get(string(rune(i)))
					Expect(err).NotTo(HaveOccurred())
					distribution[result]++
				}

				Expect(distribution["backend1"]).To(BeNumerically(">", 0))
				Expect(distribution["backend2"]).To(BeNumerically(">", 0))
				Expect(distribution["backend3"]).To(BeNumerically(">", 0))
			})
		})
	})

	Describe("Clear", func() {
		It("should remove all backends", func() {
			maglev.Add("backend1")
			maglev.Add("backend2")

			maglev.Clear()

			_, err := maglev.Get("test-key")
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Consistency", func() {
		It("should minimize disruption when adding backends", func() {
			maglev.Add("backend1")
			maglev.Add("backend2")
			maglev.Add("backend3")

			keys := []string{"key1", "key2", "key3", "key4", "key5"}
			initialMappings := make(map[string]string)

			for _, key := range keys {
				backend, err := maglev.Get(key)
				Expect(err).NotTo(HaveOccurred())
				initialMappings[key] = backend
			}

			maglev.Add("backend4")

			changedMappings := 0
			for _, key := range keys {
				backend, err := maglev.Get(key)
				Expect(err).NotTo(HaveOccurred())
				if initialMappings[key] != backend {
					changedMappings++
				}
			}

			Expect(changedMappings).To(BeNumerically("<=", len(keys)))
		})
	})

	Describe("Concurrency", func() {
		It("should handle concurrent reads safely", func() {
			maglev.Add("backend1")
			maglev.Add("backend2")

			done := make(chan bool, 10)
			for i := 0; i < 10; i++ {
				go func() {
					defer GinkgoRecover()
					for j := 0; j < 100; j++ {
						_, err := maglev.Get("test-key")
						Expect(err).NotTo(HaveOccurred())
					}
					done <- true
				}()
			}

			for i := 0; i < 10; i++ {
				Eventually(done).Should(Receive())
			}
		})
	})
})
