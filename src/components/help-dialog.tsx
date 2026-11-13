"use client";

import { Mail } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";

const SUPPORT_EMAIL = "akcysec@gmail.com";

/** "Help" opens a small centered box with how to get in touch. */
export function HelpDialog({ children }: { children: React.ReactNode }) {
  return (
    <Dialog>
      <DialogTrigger asChild>{children}</DialogTrigger>
      <DialogContent className="sm:max-w-sm">
        <DialogHeader>
          <DialogTitle>Need a hand?</DialogTitle>
          <DialogDescription>
            If you have any questions, run into a problem, or have an idea for MakerMetrics, shoot
            me a message at{" "}
            <a href={`mailto:${SUPPORT_EMAIL}`} className="font-medium text-foreground underline">
              {SUPPORT_EMAIL}
            </a>
            .
          </DialogDescription>
        </DialogHeader>
        <Button asChild className="w-fit">
          <a href={`mailto:${SUPPORT_EMAIL}?subject=MakerMetrics`}>
            <Mail className="size-4" />
            Send an email
          </a>
        </Button>
      </DialogContent>
    </Dialog>
  );
}
