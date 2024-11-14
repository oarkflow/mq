package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/oarkflow/mq/dag/v2"
	"log"
	"net/http"
)

func main() {
	graph := v2.NewGraph()
	customRegistrationNode := &v2.Operation{
		Type:    "page",
		ID:      "customRegistration",
		Content: `<html><body><h1>Custom Registration</h1><form method="POST" action="/submit?taskID={{taskID}}"><label>Email: </label><input type="text" name="email"><br><label>Phone: </label><input type="text" name="phone"><br><label>City: </label><input type="text" name="city"><br><input type="submit" value="Submit"></form></body></html>`,
	}
	checkValidityNode := &v2.Operation{
		Type: "process",
		ID:   "checkValidity",
		Func: func(task *v2.Task) v2.Result {
			var inputs map[string]string
			if err := json.Unmarshal(task.Payload, &inputs); err != nil {
				return v2.Result{ConditionStatus: "customRegistration", Error: fmt.Errorf("Invalid input format")}
			}

			email, phone := inputs["email"], inputs["phone"]
			if !isValidEmail(email) || !isValidPhone(phone) {
				return v2.Result{
					ConditionStatus: "customRegistration",
					Error:           fmt.Errorf("Invalid email or phone number. Please try again."),
				}
			}
			return v2.Result{ConditionStatus: "checkManualVerification"}
		},
	}
	checkManualVerificationNode := &v2.Operation{
		Type: "process",
		ID:   "checkManualVerification",
		Func: func(task *v2.Task) v2.Result {
			var inputs map[string]string
			if err := json.Unmarshal(task.Payload, &inputs); err != nil {
				return v2.Result{ConditionStatus: "customRegistration", Error: fmt.Errorf("Invalid input format")}
			}
			city := inputs["city"]
			if city != "Kathmandu" {
				return v2.Result{ConditionStatus: "manualVerificationPage"}
			}
			return v2.Result{ConditionStatus: "approveCustomer"}
		},
	}
	approveCustomerNode := &v2.Operation{
		Type: "process",
		ID:   "approveCustomer",
		Func: func(task *v2.Task) v2.Result {
			task.FinalResult = "Customer approved"
			return v2.Result{}
		},
	}
	sendVerificationEmailNode := &v2.Operation{
		Type: "process",
		ID:   "sendVerificationEmail",
		Func: func(task *v2.Task) v2.Result {
			return v2.Result{}
		},
	}
	verificationLinkPageNode := &v2.Operation{
		Type:    "page",
		ID:      "verificationLinkPage",
		Content: `<html><body><h1>Verify Email</h1><p>Click here to verify your email</p><a href="/verify?taskID={{taskID}}">Verify</a></body></html>`,
	}
	dashboardNode := &v2.Operation{
		Type:    "page",
		ID:      "dashboard",
		Content: `<html><body><h1>Dashboard</h1><p>Welcome to your dashboard!</p></body></html>`,
	}
	manualVerificationNode := &v2.Operation{
		Type:    "page",
		ID:      "manualVerificationPage",
		Content: `<html><body><h1>Manual Verification</h1><p>Please verify the user's information manually.</p><form method="POST" action="/verify?taskID={{taskID}}"><input type="submit" value="Approve"></form></body></html>`,
	}
	verifyApprovedNode := &v2.Operation{
		Type: "process",
		ID:   "verifyApproved",
		Func: func(task *v2.Task) v2.Result {
			return v2.Result{}
		},
	}
	denyVerificationNode := &v2.Operation{
		Type: "process",
		ID:   "denyVerification",
		Func: func(task *v2.Task) v2.Result {
			task.FinalResult = "Verification Denied"
			return v2.Result{}
		},
	}

	graph.AddNode(customRegistrationNode)
	graph.AddNode(checkValidityNode)
	graph.AddNode(checkManualVerificationNode)
	graph.AddNode(approveCustomerNode)
	graph.AddNode(sendVerificationEmailNode)
	graph.AddNode(verificationLinkPageNode)
	graph.AddNode(dashboardNode)
	graph.AddNode(manualVerificationNode)
	graph.AddNode(verifyApprovedNode)
	graph.AddNode(denyVerificationNode)

	graph.AddEdge("customRegistration", "checkValidity")
	graph.AddEdge("checkValidity", "checkManualVerification")
	graph.AddEdge("checkManualVerification", "approveCustomer")
	graph.AddEdge("checkManualVerification", "manualVerificationPage")
	graph.AddEdge("approveCustomer", "sendVerificationEmail")
	graph.AddEdge("sendVerificationEmail", "verificationLinkPage")
	graph.AddEdge("verificationLinkPage", "dashboard")
	graph.AddEdge("manualVerificationPage", "verifyApproved")
	graph.AddEdge("manualVerificationPage", "denyVerification")
	graph.AddEdge("verifyApproved", "approveCustomer")
	graph.AddEdge("denyVerification", "verificationLinkPage")

	http.HandleFunc("/verify", func(w http.ResponseWriter, r *http.Request) {
		verifyHandler(w, r, graph.Tm)
	})
	graph.Start()
}

func isValidEmail(email string) bool {
	return email != ""
}

func isValidPhone(phone string) bool {
	return phone != ""
}

func verifyHandler(w http.ResponseWriter, r *http.Request, tm *v2.TaskManager) {
	taskID := r.URL.Query().Get("taskID")
	if taskID == "" {
		http.Error(w, "Missing taskID", http.StatusBadRequest)
		return
	}
	task, exists := tm.GetTask(taskID)
	if !exists {
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}
	data := map[string]any{
		"email_verified": "true",
	}
	bt, _ := json.Marshal(data)
	task.Payload = bt
	log.Printf("Email for taskID %s successfully verified.", task.ID)
	nextNode, exists := tm.Graph.Nodes["dashboard"]
	if !exists {
		http.Error(w, "Dashboard Operation not found", http.StatusInternalServerError)
		return
	}
	result := nextNode.ProcessTask(context.Background(), task)
	if result.Error != nil {
		http.Error(w, result.Error.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, string(result.Payload))
}
